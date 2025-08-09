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
