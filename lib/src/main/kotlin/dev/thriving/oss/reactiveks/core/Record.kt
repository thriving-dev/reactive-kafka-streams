package dev.thriving.oss.reactiveks.core

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
