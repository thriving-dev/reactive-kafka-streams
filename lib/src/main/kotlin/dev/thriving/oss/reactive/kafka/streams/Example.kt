package dev.thriving.oss.reactive.kafka.streams

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

fun main() {
    val kafkaProps = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
        ConsumerConfig.GROUP_ID_CONFIG to "my-reactive-group",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        "internal.leave.group.on.close" to "true",
    )

    val builder = ReactiveStreamsBuilder()
    val stream = builder.stream<String, String>("input-topic")
        .filter { key, value -> value.length > 3 }
        .map { key, value -> value.uppercase() }
        .to("output-topic")

    val topology = builder.buildTopology()
    val streams = ReactiveKafkaStreams(topology, kafkaProps)

    // Start processing
    streams.start()

    // Add shutdown hook
    Runtime.getRuntime().addShutdownHook(Thread {
        streams.close()
    })
}

