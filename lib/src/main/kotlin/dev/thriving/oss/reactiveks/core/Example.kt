package dev.thriving.oss.reactiveks.core

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

fun main() {
    val builder = ReactiveStreamsBuilder()

    builder
        .stream<String, ByteArray>("input-topic")
        .peek { r -> println("[peek in] key=${r.key} val=${r.value?.decodeToString()}") }
        .filter { rec ->
            rec.value != null && rec.value.decodeToString().length > 3 }
        .map { rec ->
            // example: pass-through key, uppercase value
            val newVal = rec.value!!.map { b -> b.toInt().toChar() }.joinToString("").uppercase().toByteArray()
            Record(rec.key, newVal, timestamp = rec.timestamp)
        }
        .peek { r -> println("[peek out] key=${r.key} val=${r.value?.decodeToString()}") }
        .to(topic = "output-topic", builder = builder)

    val topology = builder.build()

    val consumerProps = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
        ConsumerConfig.GROUP_ID_CONFIG to "rks-demo-13",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG to "3000",
        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG to "10000",
        "internal.leave.group.on.close" to "true",
    )

    val producerProps = mapOf(
        // you can omit this if you want it copied from consumerProps by the patch above
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
        // optional, you can rely on defaults in code
        ProducerConfig.ACKS_CONFIG to "all",
    )

    val streams = ReactiveKafkaStreams(
        topology = topology,
        consumerProps = consumerProps,
        producerProps = producerProps,
    )

    // add a shutdown hook, then block
    Runtime.getRuntime().addShutdownHook(Thread { streams.close() })

    streams.start()
    streams.awaitTermination()
}
