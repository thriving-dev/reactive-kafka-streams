package dev.thriving.oss.reactiveks.core

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference


enum class ReactiveKafkaStreamsState { CREATED, RUNNING, STOPPING, STOPPED, ERROR }

class ReactiveKafkaStreams(
    private val topology: ReactiveTopology,
    private val consumerProps: Map<String, Any?>,
    private val producerProps: Map<String, Any?> = emptyMap(),
    private val config: ReactiveKsConfig = ReactiveKsConfig(),
) : AutoCloseable {

    private val watermarks = ConcurrentHashMap<TaskId, TaskCommitWatermark>()
    fun taskWatermarks(): Map<TaskId, TaskCommitWatermark> = watermarks.toMap()

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
            this[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = "true"
            putIfAbsent(ProducerConfig.ACKS_CONFIG, "all")
            putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, 5)
            putIfAbsent(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024)
//            putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
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
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to (consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG]
                    ?: "earliest"),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false
            ).apply {
                putAll(consumerProps)
                if (config.enableReadCommitted) {
                    this[ConsumerConfig.ISOLATION_LEVEL_CONFIG] = "read_committed"
                }
            }

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

            val groupId = (consumerProps[ConsumerConfig.GROUP_ID_CONFIG] ?: "reactive-ks").toString()

            // Group by TopicPartition → create a StreamTask per partition group
            val disposable =
                receiver.receive()
                    .groupBy { it.receiverOffset().topicPartition() } // -> GroupedFlux<TopicPartition, ReceiverRecord>
                    .flatMap { group ->
                        val tp = group.key()!!
                        val id = TaskId(tp.topic(), tp.partition())
                        println("[rks] assigned $tp")

                        tasks.remove(id)?.close()

                        val task = StreamTask(
                            id = id,
                            chain = chain,
                            records = group,
                            consumerGroupId = groupId,
                            baseProducerProps = sProps,     // reuse your already-built sender props as a base
                            config = config,
                            onWatermarkCommit = { wm -> watermarks[id] = wm },
                            onFatal = { err ->
                                println("[rks] FATAL in $id: ${err.message} → shutting down")
                                this@ReactiveKafkaStreams.close()
                                state = ReactiveKafkaStreamsState.ERROR
                            }
                        )
                        tasks[id] = task

                        task.run()
                            .doFinally {
                                tasks.remove(id)?.close()
                                println("[rks] task $id completed")
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

/**
 * Similar in spirit to org.apache.kafka.streams.processor.api.Record,
 * but minimal for our purposes.
 */
data class Record<K, V>(
    val key: K?,
    val value: V?,
    val timestamp: Long? = null,   // optional override for produced record
    val headers: List<Pair<String, ByteArray>> = emptyList()
)

class ReactiveStreamsBuilder {

    fun <K, V> stream(topic: String): ReactiveKStream<K, V> {
        val source = SourceNode<K, V>(SourceId("source-$topic"), topic)
        return ReactiveKStream(source, mutableListOf())
    }

    fun build(): ReactiveTopology = ReactiveTopology().also { topo ->
        _pendingChains.forEach { topo.addChain(it) }
    }

    // internal registration from KStream.to(...)
    internal fun <K, V> registerChain(chain: ReactiveTopology.Chain<K, V>) {
        _pendingChains += chain
    }

    private val _pendingChains = mutableListOf<ReactiveTopology.Chain<*, *>>()
}

/** A minimal, chainable KStream-like API supporting filter / map / to. */
class ReactiveKStream<K, V> internal constructor(
    private val source: SourceNode<K, V>,
    private val steps: MutableList<ProcessorNode<*, *, *, *>>
) {
    fun filter(predicate: (Record<K, V>) -> Boolean): ReactiveKStream<K, V> {
        steps += FilterNode<K, V>(ProcessorId("filter-${steps.size + 1}"), predicate)
        return this
    }

    fun <K2, V2> map(mapper: (Record<K, V>) -> Record<K2, V2>): ReactiveKStream<K2, V2> {
        steps += MapNode<K, V, K2, V2>(ProcessorId("map-${steps.size + 1}"), mapper)
        @Suppress("UNCHECKED_CAST")
        return ReactiveKStream(source as SourceNode<K2, V2>, steps)
    }

    fun peek(action: (Record<K, V>) -> Unit): ReactiveKStream<K, V> {
        steps += PeekNode<K, V>(ProcessorId("peek-${steps.size + 1}"), action)
        return this
    }

    fun to(topic: String, builder: ReactiveStreamsBuilder) {
        val sink = SinkNode<K, V>(SinkId("sink-$topic"), topic)
        builder.registerChain(
            ReactiveTopology.Chain(
                source = source,
                processors = steps.toList(),
                sink = sink
            )
        )
    }
}

sealed interface NodeId {
    val id: String
}

data class SourceId(override val id: String) : NodeId
data class ProcessorId(override val id: String) : NodeId
data class SinkId(override val id: String) : NodeId

sealed class ProcessorNode<KIn, VIn, KOut, VOut>(val id: ProcessorId) {
    abstract fun apply(rec: Record<KIn, VIn>): Record<KOut, VOut>?
}

class MapNode<KIn, VIn, KOut, VOut>(
    id: ProcessorId,
    private val mapper: (Record<KIn, VIn>) -> Record<KOut, VOut>
) : ProcessorNode<KIn, VIn, KOut, VOut>(id) {
    override fun apply(rec: Record<KIn, VIn>): Record<KOut, VOut>? = mapper(rec)
}

class FilterNode<K, V>(
    id: ProcessorId,
    private val predicate: (Record<K, V>) -> Boolean
) : ProcessorNode<K, V, K, V>(id) {
    override fun apply(rec: Record<K, V>): Record<K, V>? = if (predicate(rec)) rec else null
}

class PeekNode<K, V>(
    id: ProcessorId,
    private val action: (Record<K, V>) -> Unit
) : ProcessorNode<K, V, K, V>(id) {
    override fun apply(rec: Record<K, V>): Record<K, V>? {
        try {
            action(rec)        // side-effect only
        } catch (e: Throwable) {
            // Don't kill the stream because of peek side-effects
            // Replace with proper logging if you have one
            println("[rks] peek error: ${e.message}")
        }
        return rec            // pass-through unchanged
    }
}

data class SourceNode<K, V>(
    val id: SourceId,
    val topic: String
)

data class SinkNode<K, V>(
    val id: SinkId,
    val topic: String
)

/**
 * Extremely simple topology: tracks a single linear chain per stream for now.
 * We can generalize to a DAG later.
 */
class ReactiveTopology internal constructor() {

    data class Chain<K, V>(
        val source: SourceNode<K, V>,
        val processors: List<ProcessorNode<*, *, *, *>>,
        val sink: SinkNode<*, *>?
    )

    internal val chains = mutableListOf<Chain<*, *>>()

    internal fun <K, V> addChain(chain: Chain<K, V>) {
        chains += chain
    }
}

data class TaskId(val topic: String, val partition: Int) {
    override fun toString(): String = "$topic-$partition"
}

data class ReactiveKsConfig(
    val commitIntervalMs: Long = 1000L,   // like commit.interval.ms
    val maxBatchSize: Int = 100,
    val transactionalIdPrefix: String = "rks-task",
    val enableReadCommitted: Boolean = true
)

data class TaskCommitWatermark(
    val topic: String,
    val partition: Int,
    val lastCommittedOffset: Long // inclusive
)

private data class OutMsg(
    val pr: ProducerRecord<Any?, Any?>,
    val rr: ReceiverRecord<Any?, Any?>
)
private data class Batch(
    val messages: List<OutMsg>,
    val lastOffsetsByTp: Map<TopicPartition, Long>
)

enum class TaskState { CREATED, RUNNING, CLOSED, ERROR }

class StreamTask(
    val id: TaskId,
    private val chain: ReactiveTopology.Chain<Any?, Any?>,
    private val records: Flux<ReceiverRecord<Any?, Any?>>,
    private val consumerGroupId: String,
    private val baseProducerProps: Map<String, Any?>,
    private val config: ReactiveKsConfig,
    private val onWatermarkCommit: (TaskCommitWatermark) -> Unit,
    private val onFatal: (Throwable) -> Unit
) : AutoCloseable {

    private val state = AtomicReference(TaskState.CREATED)
    private val cancel = Sinks.empty<Void>()

    // Per-task transactional sender
    private val sender: KafkaSender<Any?, Any?> by lazy {
        val props = HashMap<String, Any?>(baseProducerProps).apply {
            this[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
            this[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = ByteArraySerializer::class.java
            this[ProducerConfig.ACKS_CONFIG] = "all"
            this[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = true
            this[ProducerConfig.TRANSACTIONAL_ID_CONFIG] =
                "${config.transactionalIdPrefix}-${id.topic}-${id.partition}"
        }
        KafkaSender.create(SenderOptions.create<Any?, Any?>(props))
    }

    fun run(): Mono<TaskId> {
        if (!state.compareAndSet(TaskState.CREATED, TaskState.RUNNING)) {
            return Mono.error(IllegalStateException("Task $id not in CREATED"))
        }
        val sinkTopic = (chain.sink as SinkNode<Any?, Any?>).topic

        val batchFlux: Flux<Batch> =
            records
                .takeUntilOther(cancel.asMono())
                .mapNotNull { rr ->
                    val out = try { applyChain(chain, rr.toRecord()) } catch (e: Throwable) {
                        println("[task $id] processor error: ${e.message}")
                        null // drop this record; no offset commit, it will reprocess after restart if fatal happens later
                    }
                    if (out == null) null else {
                        val k = out.key; val v = out.value
                        require(k == null || k is String) { "Key must be String" }
                        require(v == null || v is ByteArray) { "Value must be ByteArray" }
                        OutMsg(out.toProducerRecord(sinkTopic), rr)
                    }
                }
                .bufferTimeout(config.maxBatchSize, Duration.ofMillis(config.commitIntervalMs))
                //.map { msgs -> msgs.filterNotNull() }
                .map { msgs -> msgs as MutableList<OutMsg> } // note: using mapNotNull above does not set the correct type
                .filter { it.isNotEmpty() }
                .map { msgs ->
                    val last = HashMap<TopicPartition, Long>(1)
                    msgs.forEach { m ->
                        val tp = m.rr.receiverOffset().topicPartition()
                        val off = m.rr.receiverOffset().offset()
                        last.merge(tp, off) { a, b -> maxOf(a, b) }
                    }
                    Batch(msgs, last)
                }

        // Process batches sequentially using the transactional manager.
        return batchFlux
            .concatMap { batch -> processBatchTransactional(batch) }
            .doOnSubscribe { println("[task $id] RUNNING (tx-batching, EOS)") }
            .doOnError { e ->
                state.set(TaskState.ERROR)
                println("[task $id] FATAL: ${e.message}")
                onFatal(e) // shut down runtime
            }
            .doFinally {
                try { sender.close() } catch (_: Throwable) {}
                if (state.get() != TaskState.ERROR) state.set(TaskState.CLOSED)
                println("[task $id] TERMINATED state=${state.get()}")
            }
            .then(Mono.just(id))
    }

    /** One transactional batch: begin → send → sendOffsetsToTransaction → commit. */
    private fun processBatchTransactional(batch: Batch): Mono<Void> {
        val tm = sender.transactionManager()

        // Prepare outbound sends
        val outbound: Flux<SenderRecord<Any?, Any?, ReceiverRecord<Any?, Any?>>> =
            Flux.fromIterable(batch.messages)
                .map { m -> SenderRecord.create(m.pr, m.rr) }

        // Offsets to commit inside the transaction: note the +1 semantics
        val offsets = batch.lastOffsetsByTp.mapValues { (_, off) -> OffsetAndMetadata(off + 1) }

        return tm.begin<Void>() // Mono<Void>
            .thenMany(sender.send(outbound)) // Flux<SenderResult<ReceiverRecord<..>>>
            .then<Void>(
                // commit the consumer offsets inside the same transaction
                tm.sendOffsets(offsets, consumerGroupId)
            )
            .then<Void>(
                Mono.fromRunnable<Void> { /* barrier point to serialize commit call */ }
                    .then(Mono.defer<Void> { tm.commit() }) // Mono<Void> (commit is async)
                    .subscribeOn(Schedulers.boundedElastic())
            )
            .doOnSuccess {
                // After tx commit succeeded, update watermark (inclusive)
                batch.lastOffsetsByTp.forEach { (tp, off) ->
                    onWatermarkCommit(TaskCommitWatermark(tp.topic(), tp.partition(), off))
                }
            }
            .onErrorResume { e ->
                tm.abort<Void>()
                    .onErrorResume { _ -> Mono.empty() } // swallow abort errors
                    .then(Mono.error(IllegalStateException("Transactional batch failed on $id: ${e.message}", e)))
            }
    }

    override fun close() {
        if (state.get() == TaskState.RUNNING) cancel.tryEmitEmpty()
    }

    // --- helpers (unchanged shape) ---
    private fun <K, V> ReceiverRecord<K, V>.toRecord(): Record<K, V> =
        Record(key(), value(), timestamp(), headers().map { it.key() to it.value() })
    private fun <K, V> Record<K, V>.toProducerRecord(topic: String): ProducerRecord<K?, V?> {
        val pr = ProducerRecord(topic, null, timestamp, key, value)
        headers.forEach { (k, v) -> pr.headers().add(k, v) }
        return pr
    }
    private fun <K, V> applyChain(
        chain: ReactiveTopology.Chain<K, V>,
        input: Record<K, V>
    ): Record<Any?, Any?>? {
        var curr: Any? = input
        for (p in chain.processors) {
            @Suppress("UNCHECKED_CAST")
            val proc = p as ProcessorNode<Any?, Any?, Any?, Any?>
            curr = proc.apply(curr as Record<Any?, Any?>) ?: return null
        }
        @Suppress("UNCHECKED_CAST")
        return curr as Record<Any?, Any?>
    }
}

