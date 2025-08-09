package dev.thriving.oss.reactiveks.core

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult
import java.util.concurrent.atomic.AtomicReference

data class TaskId(val topic: String, val partition: Int) {
    override fun toString(): String = "$topic-$partition"
}
enum class TaskState { CREATED, RUNNING, CLOSED, ERROR }

class StreamTask(
    val id: TaskId,
    private val chain: ReactiveTopology.Chain<Any?, Any?>,
    private val records: Flux<ReceiverRecord<Any?, Any?>>,
    private val sender: KafkaSender<Any?, Any?>
) : AutoCloseable {

    private val state = AtomicReference(TaskState.CREATED)
    private val cancel = Sinks.empty<Void>() // signal to stop this task

    /** Build and return a completion Mono; caller subscribes. */
    fun run(): Mono<TaskId> {
        if (!state.compareAndSet(TaskState.CREATED, TaskState.RUNNING)) {
            return Mono.error(IllegalStateException("Task $id not in CREATED"))
        }
        val sinkTopic = (chain.sink as SinkNode<Any?, Any?>).topic

        return records
            .takeUntilOther(cancel.asMono())            // allow external close()
            .flatMap { rr ->
                val inRec = rr.toRecord()
                val outRec = try { applyChain(chain, inRec) }
                catch (e: Throwable) {
                    println("[task $id] processor error: ${e.message}")
                    rr.receiverOffset().acknowledge()
                    null
                }
                if (outRec == null) {
                    rr.receiverOffset().acknowledge()
                    Mono.empty<SenderResult<Void>>()
                } else {
                    val k = outRec.key; val v = outRec.value
                    if (k != null && k !is String)
                        return@flatMap Mono.error<SenderResult<Void>>(IllegalStateException("Key must be String"))
                    if (v != null && v !is ByteArray)
                        return@flatMap Mono.error<SenderResult<Void>>(IllegalStateException("Value must be ByteArray"))

                    val pr: ProducerRecord<Any?, Any?> = outRec.toProducerRecord(sinkTopic)
                    val sr = SenderRecord.create(pr, rr)
                    sender.send(Flux.just(sr))
                        .doOnNext { it.correlationMetadata().receiverOffset().acknowledge() }
                        .single()
                }
            }
            .doOnSubscribe { println("[task $id] RUNNING") }
            .doOnError { e ->
                state.set(TaskState.ERROR)
                println("[task $id] ERROR: ${e.message}")
            }
            .doFinally {
                if (state.get() != TaskState.ERROR) state.set(TaskState.CLOSED)
                println("[task $id] TERMINATED state=${state.get()}")
            }
            .then(Mono.just(id)) // completion signal
    }

    override fun close() {
        if (state.get() == TaskState.RUNNING) {
            cancel.tryEmitEmpty()
        }
    }

    // --- helpers ---
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

//import org.apache.kafka.clients.producer.ProducerRecord
//import org.apache.kafka.common.TopicPartition
//import reactor.core.Disposable
//import reactor.core.publisher.Flux
//import reactor.kafka.receiver.ReceiverRecord
//import reactor.kafka.sender.KafkaSender
//import reactor.kafka.sender.SenderRecord
//import reactor.kafka.sender.SenderResult
//import java.util.concurrent.atomic.AtomicReference
//
//data class TaskId(val topic: String, val partition: Int) {
//    override fun toString(): String = "$topic-$partition"
//}
//
//enum class TaskState { CREATED, RUNNING, PAUSED, CLOSED, ERROR }
//
///**
// * Owns processing for exactly one TopicPartition.
// * - Applies the processor chain (filter/map/peek…)
// * - Produces to sink
// * - Acks offsets only after a successful produce (at-least-once)
// */
//class StreamTask(
//    val id: TaskId,
//    private val chain: ReactiveTopology.Chain<Any?, Any?>,
//    private val records: Flux<ReceiverRecord<Any?, Any?>>,
//    private val sender: KafkaSender<Any?, Any?>,
//) : AutoCloseable {
//
//    private val _state = AtomicReference(TaskState.CREATED)
//    private var subscription: Disposable? = null
//
//    fun state(): TaskState = _state.get()
//
//    fun start() {
//        if (!_state.compareAndSet(TaskState.CREATED, TaskState.RUNNING)) return
//        val sinkTopic = (chain.sink as SinkNode<Any?, Any?>).topic
//
//        val d = records
//            .flatMap { rr ->
//                val inRec = rr.toRecord()
//                val outRec = try {
//                    applyChain(chain, inRec)
//                } catch (e: Throwable) {
//                    println("[task ${id}] processor error: ${e.message}")
//                    rr.receiverOffset().acknowledge()
//                    null
//                }
//
//                if (outRec == null) {
//                    rr.receiverOffset().acknowledge()
//                    reactor.core.publisher.Mono.empty<SenderResult<Void>>()
//                } else {
//                    // Type guard against default serializers (String/ByteArray)
//                    val k = outRec.key
//                    val v = outRec.value
//                    if (k != null && k !is String) {
//                        return@flatMap reactor.core.publisher.Mono.error<SenderResult<Void>>(
//                            IllegalStateException("Key must be String; got ${k!!::class.java.simpleName}")
//                        )
//                    }
//                    if (v != null && v !is ByteArray) {
//                        return@flatMap reactor.core.publisher.Mono.error<SenderResult<Void>>(
//                            IllegalStateException("Value must be ByteArray; got ${v!!::class.java.simpleName}")
//                        )
//                    }
//
//                    val pr: ProducerRecord<Any?, Any?> = outRec.toProducerRecord(sinkTopic)
//                    val sr = SenderRecord.create(pr, rr) // correlate back to input
//
//                    sender.send(Flux.just(sr))
//                        .doOnNext {
//                            it.correlationMetadata().receiverOffset().acknowledge()
//                        }
//                        .single()
//                }
//            }
//            .doOnSubscribe { println("[task $id] RUNNING") }
//            .doOnError { e ->
//                _state.set(TaskState.ERROR)
//                println("[task $id] ERROR: ${e.message}")
//            }
//            .doOnTerminate {
//                if (_state.get() != TaskState.ERROR) _state.set(TaskState.CLOSED)
//                println("[task $id] TERMINATED with state=${_state.get()}")
//            }
//            .subscribe()
//
//        subscription = d
//    }
//
//    fun pause() {
//        if (_state.compareAndSet(TaskState.RUNNING, TaskState.PAUSED)) {
//            subscription?.dispose()
//            subscription = null
//            println("[task $id] PAUSED")
//        }
//    }
//
//    override fun close() {
//        val prev = _state.getAndSet(TaskState.CLOSED)
//        if (prev != TaskState.CLOSED) {
//            subscription?.dispose()
//            subscription = null
//            println("[task $id] CLOSED")
//        }
//    }
//
//    // --- helpers (same as before) ---
//
//    private fun <K, V> ReceiverRecord<K, V>.toRecord(): Record<K, V> =
//        Record(key(), value(), timestamp(), headers().map { it.key() to it.value() })
//
//    private fun <K, V> Record<K, V>.toProducerRecord(topic: String): ProducerRecord<K?, V?> {
//        val pr = ProducerRecord(topic, null, timestamp, key, value)
//        headers.forEach { (k, v) -> pr.headers().add(k, v) }
//        return pr
//    }
//
//    private fun <K, V> applyChain(
//        chain: ReactiveTopology.Chain<K, V>,
//        input: Record<K, V>
//    ): Record<Any?, Any?>? {
//        var curr: Any? = input
//        for (p in chain.processors) {
//            @Suppress("UNCHECKED_CAST")
//            val proc = p as ProcessorNode<Any?, Any?, Any?, Any?>
//            curr = when (val res = proc.apply(curr as Record<Any?, Any?>)) {
//                null -> return null
//                else -> res
//            }
//        }
//        @Suppress("UNCHECKED_CAST")
//        return curr as Record<Any?, Any?>
//    }
//}
