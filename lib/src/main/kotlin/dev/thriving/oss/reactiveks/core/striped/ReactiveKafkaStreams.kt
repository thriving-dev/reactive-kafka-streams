@file:Suppress("UNCHECKED_CAST")

package dev.thriving.oss.reactiveks.core.striped

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
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOffset
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverPartition
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult
import reactor.kotlin.core.publisher.toFlux
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

/* ============== Data & Utilities ============== */

data class Record<K, V>(
    val key: K?,
    val value: V,
    val timestamp: Long,
    val headers: Map<String, ByteArray> = emptyMap()
)

data class TaskId(val topic: String, val partition: Int)
data class TaskCommitWatermark(
    @Volatile var lastDelivered: Long = -1L,     // last processed offset
    @Volatile var lastCommitted: Long = -1L      // last committed offset
)

private fun positiveHash(i: Int) = i and 0x7fffffff
private fun <K> stripeOf(key: K?, stripes: Int): Int =
    if (stripes <= 1) 0 else if (key == null) 0 else positiveHash(key.hashCode()) % stripes

/* ============== Topology & DSL ============== */

interface ProcessorNode<IK, IV, OK, OV> {
    val name: String
    val mayChangeKey: Boolean
    fun apply(input: Record<IK, IV>): PublisherLike<Record<OK, OV>>
}

// Tiny façade so we can return Mono/Flux without tying generics too hard
sealed interface PublisherLike<T> {
    fun asFlux(): Flux<T>
}
private class MonoLike<T>(private val mono: Mono<T>) : PublisherLike<T> {
    override fun asFlux(): Flux<T> = mono.flux()
}
private class FluxLike<T>(private val flux: Flux<T>) : PublisherLike<T> {
    override fun asFlux(): Flux<T> = flux
}

/* ---- Concrete nodes (common DSL ops) ---- */

class FilterNode<K, V>(
    override val name: String,
    private val predicate: (Record<K, V>) -> Boolean
) : ProcessorNode<K, V, K, V> {
    override val mayChangeKey: Boolean = false
    override fun apply(input: Record<K, V>): PublisherLike<Record<K, V>> =
        if (predicate(input)) MonoLike(Mono.just(input)) else MonoLike(Mono.empty())
}

class MapValuesNode<K, VI, VO>(
    override val name: String,
    private val f: (VI) -> VO
) : ProcessorNode<K, VI, K, VO> {
    override val mayChangeKey: Boolean = false
    override fun apply(input: Record<K, VI>): PublisherLike<Record<K, VO>> =
        MonoLike(Mono.just(Record(key = input.key, value = f(input.value), timestamp = input.timestamp, headers = input.headers)))
}

class PeekNode<K, V>(
    override val name: String,
    private val action: (Record<K, V>) -> Unit
) : ProcessorNode<K, V, K, V> {
    override val mayChangeKey: Boolean = false
    override fun apply(input: Record<K, V>): PublisherLike<Record<K, V>> {
        action(input)
        return MonoLike(Mono.just(input))
    }
}

/** Key-changing: select a new key from the current record */
class SelectKeyNode<KI, VI, KO>(
    override val name: String,
    private val keyFn: (Record<KI, VI>) -> KO?
) : ProcessorNode<KI, VI, KO, VI> {
    override val mayChangeKey: Boolean = true
    override fun apply(input: Record<KI, VI>): PublisherLike<Record<KO, VI>> =
        MonoLike(Mono.just(Record(key = keyFn(input), value = input.value, timestamp = input.timestamp, headers = input.headers)))
}

/** Key & value changing map */
class MapNode<KI, VI, KO, VO>(
    override val name: String,
    private val f: (Record<KI, VI>) -> Record<KO, VO>?
) : ProcessorNode<KI, VI, KO, VO> {
    override val mayChangeKey: Boolean = true
    override fun apply(input: Record<KI, VI>): PublisherLike<Record<KO, VO>> {
        val out = f(input)
        return if (out == null) MonoLike(Mono.empty()) else MonoLike(Mono.just(out))
    }
}

/** Simple sink to a Kafka topic (value serializer outside, we take ByteArray here) */
class SinkToTopicNode<K>(
    override val name: String,
    val topic: String,
) : ProcessorNode<K, ByteArray, K, ByteArray> {
    override val mayChangeKey: Boolean = false
    override fun apply(input: Record<K, ByteArray>): PublisherLike<Record<K, ByteArray>> =
        MonoLike(Mono.just(input)) // identity; actual sending handled by runtime
}

/* ---- Topology containers ---- */

class ReactiveTopology private constructor(
    internal val chains: List<Chain<*, *>>
) {
    class Builder(private val defaultStripes: Int = Runtime.getRuntime().availableProcessors()) {
        private val chains = mutableListOf<Chain<*, *>>()

        fun <K, V> stream(sourceTopic: String): KStream<K, V> {
            val c = Chain<K, V>(sourceTopic = sourceTopic, defaultStripes = defaultStripes)
            chains += c
            return KStream(c)
        }

        fun build(): ReactiveTopology = ReactiveTopology(chains.toList())
    }

    /** A linear chain for now; branches can be added later */
    class Chain<K, V>(
        val sourceTopic: String,
        val defaultStripes: Int
    ) {
        internal val nodes = mutableListOf<ProcessorNode<*, *, *, *>>()
        internal var sink: SinkToTopicNode<*>? = null

        fun <KI, VI, KO, VO> add(node: ProcessorNode<KI, VI, KO, VO>): Chain<KO, VO> {
            nodes += node
            return this as Chain<KO, VO>
        }

        fun sink(node: SinkToTopicNode<*>) {
            sink = node
            nodes += node
        }

        override fun toString(): String =
            "Chain(source=$sourceTopic, nodes=${nodes.map { it.javaClass.simpleName + '(' + it.name + ')' }})"
    }
}

/* ---- DSL façade ---- */

class KStream<K, V>(private val chain: ReactiveTopology.Chain<K, V>) {
    fun filter(name: String = "filter", p: (Record<K, V>) -> Boolean): KStream<K, V> =
        KStream(chain.add(FilterNode(name, p)))

    fun mapValues(name: String = "mapValues", f: (V) -> V): KStream<K, V> =
        KStream(chain.add(MapValuesNode(name, f)))

    fun peek(name: String = "peek", a: (Record<K, V>) -> Unit): KStream<K, V> =
        KStream(chain.add(PeekNode(name, a)))

    fun <KO> selectKey(name: String = "selectKey", f: (Record<K, V>) -> KO?): KStream<KO, V> =
        KStream(chain.add(SelectKeyNode(name, f)))

    fun <KO, VO> map(name: String = "map", f: (Record<K, V>) -> Record<KO, VO>?): KStream<KO, VO> =
        KStream(chain.add(MapNode(name, f)))

    /** Terminal */
    fun to(topic: String, name: String = "to") {
        chain.sink(SinkToTopicNode<K>(name = name, topic = topic))
    }
}

/* ============== Segments (Ordering Domains) ============== */

data class Segment(
    val name: String,
    val nodes: List<ProcessorNode<Any?, Any?, Any?, Any?>>,
    val stripes: Int
)

/** Split a linear chain into segments on key boundaries (nodes that may change key). */
private fun compileToSegments(chain: ReactiveTopology.Chain<*, *>): List<Segment> {
    val segments = mutableListOf<Segment>()
    val buf = mutableListOf<ProcessorNode<Any?, Any?, Any?, Any?>>()
    var segIdx = 0

    fun flush() {
        if (buf.isNotEmpty()) {
            segments += Segment(
                name = "segment-$segIdx",
                nodes = buf.toList(),
                stripes = chain.defaultStripes
            )
            buf.clear()
            segIdx++
        }
    }

    chain.nodes.forEach { raw ->
        val n = raw as ProcessorNode<Any?, Any?, Any?, Any?>
        buf += n
        if (n.mayChangeKey) flush()
    }
    flush()
    return segments
}

/* ============== Runtime ============== */

data class ReactiveKsConfig(
    val defaultStripes: Int = Runtime.getRuntime().availableProcessors(),
    val commitInterval: Duration = Duration.ofSeconds(5),
    val maxInFlightPerStripe: Int = 128
)

enum class ReactiveKafkaStreamsState { CREATED, RUNNING, ERROR, PENDING_SHUTDOWN, DEAD }

class ReactiveKafkaStreams(
    private val topology: ReactiveTopology,
    private val consumerProps: Map<String, Any?> = emptyMap(),
    private val producerProps: Map<String, Any?> = emptyMap(),
    private val config: ReactiveKsConfig = ReactiveKsConfig(),
) : AutoCloseable {

    private val watermarks = ConcurrentHashMap<TaskId, TaskCommitWatermark>()
    fun taskWatermarks(): Map<TaskId, TaskCommitWatermark> = watermarks.toMap()

    private val started = AtomicBoolean(false)
    @Volatile private var state: ReactiveKafkaStreamsState = ReactiveKafkaStreamsState.CREATED
    private val subscriptions = mutableListOf<Disposable>()
    private var sender: KafkaSender<Any?, Any?>? = null
    private val tasks = ConcurrentHashMap<TaskId, StreamTask>()

    fun state(): ReactiveKafkaStreamsState = state

    fun start() {
        check(started.compareAndSet(false, true)) { "Already started" }
        state = ReactiveKafkaStreamsState.RUNNING

        // Kafka Sender
        val sProps = mutableMapOf<String, Any?>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to requireNotNull(consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG]) {
                "bootstrap.servers missing"
            },
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java,
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true
        ).apply {
            putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, 5)
            putIfAbsent(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024)
            putAll(producerProps)
        }
        sender = KafkaSender.create(SenderOptions.create<Any?, Any?>(sProps))
        val s = requireNotNull(sender)

        // One receiver per chain (for now), partitioned into tasks
        topology.chains.forEachIndexed { idx, raw ->
            val chain = raw as ReactiveTopology.Chain<Any?, Any?>
            require(chain.sink != null) { "Chain #$idx has no sink. Call .to(...)." }

            val rProps = mutableMapOf<String, Any?>(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to sProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG]!!,
                ConsumerConfig.GROUP_ID_CONFIG to (consumerProps[ConsumerConfig.GROUP_ID_CONFIG] ?: "reactive-ks"),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to (consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] ?: "earliest"),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false
            ).apply { putAll(consumerProps) }

            val receiverOptions = ReceiverOptions.create<String, ByteArray>(rProps)
                .subscription(listOf(chain.sourceTopic))
                .addAssignListener { parts -> onAssigned(chain, parts) }
                .addRevokeListener { parts -> onRevoked(parts) }

            val receiver = KafkaReceiver.create(receiverOptions)

            // Per-record stream -> per-partition task demux
            val disp = receiver
                .receive()
                .groupBy { rec -> TaskId(rec.topic(), rec.partition()) }
                .flatMap({ g ->
                    val id = g.key()!!
                    val task = tasks.computeIfAbsent(id) { StreamTask(id, chain, s, config) }
                    g.concatMap { rr ->
                        val offset = rr.receiverOffset()
                        val input = Record<Any?, Any?>(
                            key = rr.key(),
                            value = rr.value(),
                            timestamp = rr.timestamp(),
                            headers = emptyMap() // copy if needed
                        )
                        task.onRecord(input, offset)
                    }
                }, /*concurrency*/ 32)
                .doOnError { e ->
                    state = ReactiveKafkaStreamsState.ERROR
                    e.printStackTrace()
                }
                .subscribe()
            subscriptions += disp
        }
    }

    private fun onAssigned(chain: ReactiveTopology.Chain<*, *>, parts: Collection<ReceiverPartition>) {
        parts.forEach { p ->
            val id = TaskId(p.topicPartition().topic(), p.topicPartition().partition())
            tasks.computeIfAbsent(id) { StreamTask(id, chain as ReactiveTopology.Chain<Any?, Any?>, requireNotNull(sender), config) }
            watermarks.putIfAbsent(id, TaskCommitWatermark())
        }
    }

    private fun onRevoked(parts: Collection<ReceiverPartition>) {
        parts.forEach { p ->
            val id = TaskId(p.topicPartition().topic(), p.topicPartition().partition())
            tasks.remove(id)?.close()
            watermarks.remove(id)
        }
    }

    override fun close() {
        state = ReactiveKafkaStreamsState.PENDING_SHUTDOWN
        subscriptions.forEach { it.dispose() }
        subscriptions.clear()
        tasks.values.forEach { it.close() }
        tasks.clear()
        sender?.close()
        state = ReactiveKafkaStreamsState.DEAD
    }

    /* -------- StreamTask -------- */

    private inner class StreamTask(
        private val id: TaskId,
        private val chain: ReactiveTopology.Chain<Any?, Any?>,
        private val sender: KafkaSender<Any?, Any?>,
        private val cfg: ReactiveKsConfig
    ) : AutoCloseable {

        private val segments = compileToSegments(chain)
        private val commitSink = Sinks.many().replay().latest<Long>() // track last delivered offset
        private val offsets = ConcurrentHashMap<TopicPartition, Long>()
        private val closed = AtomicBoolean(false)

        override fun close() {
            closed.set(true)
        }

        fun onRecord(input: Record<Any?, Any?>, offset: ReceiverOffset): Mono<TaskId> {
            if (closed.get()) return Mono.empty()
            val tp = TopicPartition(chain.sourceTopic, id.partition)
            val startFlux = Mono.just(input).flux() as Flux<Any?>

            // Run through segments
            val out = segments.fold(startFlux) { acc: Flux<Any?>, seg ->
                runSegment(seg, acc, cfg)
            }

            // Terminal: if last node is SinkToTopicNode, we send
            val sink = chain.sink as SinkToTopicNode<Any?>?
            val sendFlux =
                if (sink != null) {
                    out.cast(Record::class.java as Class<Record<Any?, ByteArray>>)
                        .flatMap { rec ->
                            val pr = ProducerRecord<Any?, Any?>(sink.topic, rec.key, rec.value)
                            val sr = SenderRecord.create(pr, null)
                            sender.send(Mono.just(sr)).next()
                        }
                } else {
                    out.then(Mono.empty<SenderResult<Void>>())
                }

            return sendFlux.toFlux()
                .doOnNext {
                    // after ack from Kafka sender (at-least-once for now)
                    offsets[tp] = offset.offset()
                    commitSink.tryEmitNext(offset.offset())
                    watermarks[id]?.apply { lastDelivered = offset.offset() }
                }
                .timeout(Duration.ofMinutes(2)) // safety
                .then(Mono.defer {
                    // opportunistic commit (time-based commit loop could be added)
                    commitOffsets(offset)
                    Mono.just(id)
                })
        }

        private fun commitOffsets(offset: ReceiverOffset) {
            try {
                offset.commit().block(cfg.commitInterval)
                val tp = TopicPartition(chain.sourceTopic, id.partition)
                watermarks[id]?.let {
                    it.lastCommitted = offsets[tp] ?: it.lastCommitted
                }
            } catch (e: Exception) {
                state = ReactiveKafkaStreamsState.ERROR
                throw e
            }
        }
    }
}

/* ============== Segment execution (striped) ============== */

private fun runSegment(segment: Segment, input: Flux<Any?>, cfg: ReactiveKsConfig): Flux<Any?> {
    // compose nodes into a function
    val chain: (Any?) -> Flux<Any?> = { t ->
        segment.nodes.fold(Mono.just(t).flux()) { acc, node ->
            acc.flatMap { x ->
                node.apply(x as Record<Any?, Any?>).asFlux()
            }
        }
    }

    // Execute logic, then stripe by current key and enforce per-key order with concatMap.
// Backpressure is governed by prefetch (no extra buffer operator needed).
    return input
        .flatMap(chain)
        .groupBy { rec ->
            val r = rec as Record<Any?, Any?>
            stripeOf(r.key, segment.stripes)
        }
        .flatMap(
            { stripe ->
                // strictly sequential within a stripe; prefetch bounds in-flight items per lane
                stripe.concatMap({ Mono.just(it) }, /*prefetch*/ cfg.maxInFlightPerStripe)
            },
            /*concurrency*/ segment.stripes,
            /*prefetch*/    cfg.maxInFlightPerStripe
        )
}

/* ============== Builder entrypoint ============== */

class ReactiveStreamsBuilder(
    private val defaultStripes: Int = Runtime.getRuntime().availableProcessors()
) {
    private val builder = ReactiveTopology.Builder(defaultStripes)
    fun <K, V> stream(sourceTopic: String): KStream<K, V> = builder.stream(sourceTopic)
    fun build(): ReactiveTopology = builder.build()
}

/* ============== Example usage (commented) ============== */
/*
fun main() {
    val topo = ReactiveStreamsBuilder()
        .stream<String, ByteArray>("input-topic")
        .filter { r -> r.value.isNotEmpty() }
        .mapValues { bytes -> bytes } // no-op
        .selectKey { r -> r.key?.uppercase() }
        .map("toBytes") { rec ->
            rec.key?.let { k ->
                Record(k, "value:${k}".toByteArray(), rec.timestamp, rec.headers)
            }
        }
        .also { it.to("output-topic") }
        .let { it } // ignore

    val streams = ReactiveKafkaStreams(
        topology = topo,
        consumerProps = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
            ConsumerConfig.GROUP_ID_CONFIG to "rks-example"
        ),
        producerProps = emptyMap(),
        config = ReactiveKsConfig(defaultStripes = 8)
    )
    streams.start()

    Runtime.getRuntime().addShutdownHook(Thread { streams.close() })
}
*/
