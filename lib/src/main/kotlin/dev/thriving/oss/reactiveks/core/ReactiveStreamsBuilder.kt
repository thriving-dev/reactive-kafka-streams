package dev.thriving.oss.reactiveks.core

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
