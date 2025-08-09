package dev.thriving.oss.reactiveks.core

sealed interface NodeId { val id: String }
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
