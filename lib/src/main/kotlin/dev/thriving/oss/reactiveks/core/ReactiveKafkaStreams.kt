package dev.thriving.oss.reactiveks.core

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
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

    fun state(): ReactiveKafkaStreamsState = state

//    fun start() {
//        check(state == ReactiveKafkaStreamsState.CREATED || state == ReactiveKafkaStreamsState.STOPPED) {
//            "Cannot start from state=$state"
//        }
//        if (!started.compareAndSet(false, true)) return
//
//        // Sender
//        val sProps = mutableMapOf<String, Any?>(
//            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
//            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
//        )
//        sProps.putAll(producerProps)
//        sender = KafkaSender.create(SenderOptions.create<Any?, Any?>(sProps))
//
//        // One receiver pipeline per source chain (simple for now)
//        topology.chains.forEach { chain ->
//            @Suppress("UNCHECKED_CAST")
//            val c = chain as ReactiveTopology.Chain<Any?, Any?>
//
//            // Receiver
//            val rProps = mutableMapOf<String, Any?>(
//                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to requireNotNull(consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG]) { "bootstrap.servers missing" },
//                ConsumerConfig.GROUP_ID_CONFIG to (consumerProps[ConsumerConfig.GROUP_ID_CONFIG] ?: "reactive-ks"),
//                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
//                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
//                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to (consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] ?: "earliest"),
//                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false
//            )
//            consumerProps.forEach { (k, v) -> rProps[k] = v }
//
//            val receiver = KafkaReceiver.create<Any?, Any?>(ReceiverOptions.create<Any?, Any?>(rProps).subscription(listOf(c.source.topic)))
//            val s = requireNotNull(sender) { "sender should be initialized" }
//
//            val pipeline: Disposable =
//                receiver
//                    .receive() // Flux<ReceiverRecord<K,V>>
//                    .flatMap { rr ->
//                        val rec = rr.toRecord()
//                        val out = applyChain(c, rec)
//                        if (out == null) {
//                            // drop and ack
//                            rr.receiverOffset().acknowledge()
//                            Flux.empty<SenderResult<Void>>()
//                        } else {
//                            val pr = out.toProducerRecord(requireNotNull(c.sink?.topic) { "Missing sink topic" })
//                            val sr: SenderRecord<Any?, Any?, ReceiverRecord<Any?, Any?>> =
//                                SenderRecord.create(pr, rr) // correlation = input record
//                            s.send(Flux.just(sr))
//                                .doOnNext { it.correlationMetadata().receiverOffset().acknowledge() }
//                        }
//                    }
////                    .retryWhen { errs ->
////                        // naive retry; you can replace with backoff/retry spec later
////                        errs.delayElements(Duration.ofSeconds(1))
////                    }
//                    .subscribe(
//                        { /* onNext: ack handled above */ },
//                        { e ->
//                            state = ReactiveKafkaStreamsState.ERROR
//                            // You might want logging here.
//                        }
//                    )
//
//            subscriptions += pipeline
//        }
//
//        state = ReactiveKafkaStreamsState.RUNNING
//    }

    fun start() {
        check(state == ReactiveKafkaStreamsState.CREATED || state == ReactiveKafkaStreamsState.STOPPED) {
            "Cannot start from state=$state"
        }
        if (!started.compareAndSet(false, true)) return

        // ---- Validate early
        require(topology.chains.isNotEmpty()) { "Topology has no chains. Did you call .to(...) before build()?" }

        // ---- Sender
        val sProps = mutableMapOf<String, Any?>(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
        ).apply {
            // copy user overrides first
            putAll(producerProps)

            // ensure bootstrap.servers is present (fallback to consumer’s value)
            val bootstrap =
                this[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG]
                    ?: consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG]
                    ?: error("Producer bootstrap.servers missing. Provide via producerProps or consumerProps.")

            this[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrap

            // sensible defaults (override by passing in producerProps)
            putIfAbsent(ProducerConfig.ACKS_CONFIG, "all")
            putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, 5)
            putIfAbsent(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024) // 32KB
            putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
            // Not enabling idempotence yet; we’ll add EOS later
        }
        sender = KafkaSender.create<Any?, Any?>(SenderOptions.create(sProps))

        // ---- Build a robust receiver pipeline per chain
        topology.chains.forEachIndexed { idx, raw ->
            @Suppress("UNCHECKED_CAST")
            val chain = raw as ReactiveTopology.Chain<Any?, Any?>

            val bootstrap = consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG]
            require(bootstrap is String && bootstrap.isNotBlank()) { "bootstrap.servers missing or blank" }
            require(chain.sink != null) { "Chain #$idx has no sink. Call .to(...) on the stream." }

            val rProps = mutableMapOf<String, Any?>(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG to (consumerProps[ConsumerConfig.GROUP_ID_CONFIG] ?: "reactive-ks"),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to (consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] ?: "earliest"),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ).apply { putAll(consumerProps) }

            val receiver = KafkaReceiver.create<Any?, Any?>(
                ReceiverOptions.create<Any?, Any?>(rProps).subscription(listOf(chain.source.topic))
            )

            val retrySpec = reactor.util.retry.Retry
                .backoff(Long.MAX_VALUE, java.time.Duration.ofMillis(250))
                .maxBackoff(java.time.Duration.ofSeconds(30))
                .transientErrors(true)
                .doBeforeRetry { sig ->
                    // replace with your logger
                    println("[rks] retry ${sig.totalRetries()} after ${sig.failure()::class.simpleName}: ${sig.failure().message}")
                }

            val pipeline = receiver
                .receive()
                .doOnSubscribe { println("[rks] Chain #$idx subscribed to topic '${chain.source.topic}'") }
                .doOnCancel    { println("[rks] Chain #$idx CANCEL") }
                .doOnTerminate { println("[rks] Chain #$idx TERMINATE") }
                .flatMap { rr ->
                    val inRec = rr.toRecord()

                    val outRec = try {
                        applyChain(chain, inRec)
                    } catch (e: Throwable) {
                        println("[rks] processor error: ${e.message}")
                        rr.receiverOffset().acknowledge() // avoid stuck partition; add DLT later
                        null
                    }

                    if (outRec == null) {
                        rr.receiverOffset().acknowledge()
                        reactor.core.publisher.Mono.empty<SenderResult<Void>>()
                    } else {
                        // ---- Type guard to match default serializers
                        val k = outRec.key
                        val v = outRec.value
                        if (k != null && k !is String) {
                            return@flatMap reactor.core.publisher.Mono.error<SenderResult<Void>>(
                                IllegalStateException("Key must be String (got ${k::class.java.simpleName}). Supply a String key or configure a Serde.")
                            )
                        }
                        if (v != null && v !is ByteArray) {
                            return@flatMap reactor.core.publisher.Mono.error<SenderResult<Void>>(
                                IllegalStateException("Value must be ByteArray (got ${v::class.java.simpleName}). Supply ByteArray or configure a Serde.")
                            )
                        }

                        val sinkTopic = (chain.sink as SinkNode<Any?, Any?>).topic
                        val pr = outRec.toProducerRecord(sinkTopic)
                        val sr = SenderRecord.create(pr, rr) // correlate back to input
                        println("[rks] producing to $sinkTopic, correlation key=${sr.correlationMetadata().key()}, record=${sr}")

                        sender!!
                            .send(Flux.just(sr))
                            .doOnNext {
                                // success → now ack the input offset
                                it.correlationMetadata().receiverOffset().acknowledge()
                                println("[rks] produced to $sinkTopic, offset acked")
                            }
                            .doOnError { e ->
                                // IMPORTANT: don't ack on failure; at-least-once
                                println("[rks] produce FAILED to $sinkTopic: ${e.javaClass.simpleName}: ${e.message}")
                            }
                            .single()
                    }
                }
                .retryWhen(retrySpec)
                .subscribe(
                    { /* onNext handled */ },
                    { e ->
                        state = ReactiveKafkaStreamsState.ERROR
                        println("[rks] Chain #$idx ERROR: ${e.message}")
                    },
                    { println("[rks] Chain #$idx COMPLETE") }
                )

            subscriptions += pipeline
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
//        if (state != ReactiveKafkaStreamsState.RUNNING) return
//        state = ReactiveKafkaStreamsState.STOPPING
//        subscriptions.forEach { it.dispose() }
//        subscriptions.clear()
//        sender?.close()
//        sender = null
//        state = ReactiveKafkaStreamsState.STOPPED
        if (state != ReactiveKafkaStreamsState.RUNNING) {
            // allow idempotent close, still release latch
            closeLatch.countDown()
            return
        }
        state = ReactiveKafkaStreamsState.STOPPING
        subscriptions.forEach { it.dispose() }
        subscriptions.clear()
        sender?.close()
        sender = null
        state = ReactiveKafkaStreamsState.STOPPED
        closeLatch.countDown()
    }
}
