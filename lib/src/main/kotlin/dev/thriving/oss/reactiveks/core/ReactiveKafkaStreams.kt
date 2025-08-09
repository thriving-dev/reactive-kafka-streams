package dev.thriving.oss.reactiveks.core

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

enum class ReactiveKafkaStreamsState { CREATED, RUNNING, STOPPING, STOPPED, ERROR }

class ReactiveKafkaStreams(
    private val topology: ReactiveTopology,
    private val consumerProps: Map<String, Any?>,
    private val producerProps: Map<String, Any?> = emptyMap()
) : AutoCloseable {


    private val closeLatch = java.util.concurrent.CountDownLatch(1)
    private val started = java.util.concurrent.atomic.AtomicBoolean(false)
    @Volatile
    private var state: ReactiveKafkaStreamsState = ReactiveKafkaStreamsState.CREATED

    private val subscriptions = mutableListOf<Disposable>()
    private var sender: KafkaSender<Any?, Any?>? = null

    private val tasks = ConcurrentHashMap<TaskId, StreamTask>()

    fun state(): ReactiveKafkaStreamsState = state

    fun start() {
        check(state == ReactiveKafkaStreamsState.CREATED || state == ReactiveKafkaStreamsState.STOPPED) {
            "Cannot start from state=$state"
        }
        if (!started.compareAndSet(false, true)) return
        require(topology.chains.isNotEmpty()) { "Topology has no chains." }

        // --- Build shared sender
        val sProps = mutableMapOf<String, Any?>(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
        ).apply {
            putAll(producerProps)
            val bootstrap = this[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG]
                ?: consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG]
                ?: error("Producer bootstrap.servers missing.")
            this[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrap
            putIfAbsent(ProducerConfig.ACKS_CONFIG, "all")
            putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, 5)
            putIfAbsent(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024)
            putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
        }
        sender = KafkaSender.create(SenderOptions.create<Any?, Any?>(sProps))
        val s = requireNotNull(sender)

        // For now: one receiver per chain (still linear), but **partitioned into tasks**
        topology.chains.forEachIndexed { idx, raw ->
            @Suppress("UNCHECKED_CAST")
            val chain = raw as ReactiveTopology.Chain<Any?, Any?>
            require(chain.sink != null) { "Chain #$idx has no sink. Call .to(...)." }

            val rProps = mutableMapOf<String, Any?>(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to requireNotNull(consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG]) { "bootstrap.servers missing" },
                ConsumerConfig.GROUP_ID_CONFIG to (consumerProps[ConsumerConfig.GROUP_ID_CONFIG] ?: "reactive-ks"),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to (consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] ?: "earliest"),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false
            ).apply { putAll(consumerProps) }

            val receiver = KafkaReceiver.create<Any?, Any?>(
                ReceiverOptions.create<Any?, Any?>(rProps)
                    .subscription(listOf(chain.source.topic))
                    .addAssignListener { parts ->
                        parts.forEach { p -> println("[rks] assigned ${p.topicPartition()} for chain #$idx") }
                    }
                    .addRevokeListener { parts ->
                        parts.forEach { p ->
                            val tp = p.topicPartition()
                            val id = TaskId(tp.topic(), tp.partition())
                            tasks.remove(id)?.close()
                            println("[rks] revoked $tp → closed task $id for chain #$idx")
                        }
                    }
            )

            // Group by TopicPartition → create a StreamTask per partition group
            val disposable =
                receiver.receive()
                    .groupBy { it.receiverOffset().topicPartition() } // -> GroupedFlux<TopicPartition, ReceiverRecord>
                    .flatMap { group ->
                        val tp = group.key()!!
                        val id = TaskId(tp.topic(), tp.partition())
                        println("[rks] assigned $tp")

                        // ensure previous (if any) is closed
                        tasks.remove(id)?.close()

                        val task = StreamTask(id, chain, group, s)
                        tasks[id] = task

                        // IMPORTANT: do NOT also subscribe to `group` elsewhere.
                        task.run()
                            .doFinally { signal ->
                                // On group completion (e.g., revoke), task.run() completes
                                tasks.remove(id)?.close()
                                println("[rks] task $id completed ($signal)")
                            }
                    }
                    .retryWhen(
                        reactor.util.retry.Retry
                            .backoff(Long.MAX_VALUE, java.time.Duration.ofMillis(250))
                            .maxBackoff(java.time.Duration.ofSeconds(30))
                            .doBeforeRetry { sig ->
                                println("[rks] chain retry ${sig.totalRetries()}: ${sig.failure().message}")
                            }
                    )
                    .subscribe(
                        { /* onNext is TaskId; nothing else to do */ },
                        { e ->
                            state = ReactiveKafkaStreamsState.ERROR
                            println("[rks] chain ERROR: ${e.message}")
                        },
                        { println("[rks] chain COMPLETE") }
                    )

            subscriptions += disposable
        }

        state = ReactiveKafkaStreamsState.RUNNING
        println("[rks] All chains started. State = $state")
    }

    private fun <K, V> applyChain(chain: ReactiveTopology.Chain<K, V>, input: Record<K, V>): Record<Any?, Any?>? {
        var curr: Any? = input
        for (p in chain.processors) {
            @Suppress("UNCHECKED_CAST")
            val proc = p as ProcessorNode<Any?, Any?, Any?, Any?>
            curr = when (val res = proc.apply(curr as Record<Any?, Any?>)) {
                null -> return null
                else -> res
            }
        }
        @Suppress("UNCHECKED_CAST")
        return curr as Record<Any?, Any?>
    }

    private fun <K, V> ReceiverRecord<K, V>.toRecord(): Record<K, V> =
        Record(key(), value(), timestamp(), headers().map { it.key() to it.value() })

    private fun <K, V> Record<K, V>.toProducerRecord(topic: String): ProducerRecord<K?, V?> {
        val pr = ProducerRecord(topic, /* partition */ null, /* ts */ timestamp, key, value)
        headers.forEach { (k, v) -> pr.headers().add(k, v) }
        return pr
    }

    fun awaitTermination() {
        // Only block if we've actually started; otherwise wait until start() sets RUNNING
        while (state == ReactiveKafkaStreamsState.CREATED) {
            Thread.sleep(50)
        }
        closeLatch.await()
    }

    override fun close() {
        if (state != ReactiveKafkaStreamsState.RUNNING && state != ReactiveKafkaStreamsState.ERROR) {
            closeLatch.countDown(); return
        }
        state = ReactiveKafkaStreamsState.STOPPING
        // Stop tasks first
        tasks.values.forEach { it.close() }
        tasks.clear()
        // Then pipelines/sender
        subscriptions.forEach { it.dispose() }
        subscriptions.clear()
        sender?.close()
        sender = null
        state = ReactiveKafkaStreamsState.STOPPED
        closeLatch.countDown()
    }
}
