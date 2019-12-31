package com.michalopiola.app.kafka

import com.michalopiola.app.model.Witcher
import com.michalopiola.app.util.schoolsTopic
import com.michalopiola.app.util.witchersTopic
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import java.util.*

class StreamsProcessor(private val brokers: String, private val schemaRegistryUrl: String) {

    fun process() {
        val streamsBuilder = StreamsBuilder()
        val avroSerde = GenericAvroSerde().apply {
            configure(mapOf(Pair("schema.registry.url", schemaRegistryUrl)), false)
        }
        val witcherAvroStream: KStream<String, GenericRecord> = streamsBuilder.stream<String, GenericRecord>(witchersTopic,
            Consumed.with(Serdes.String(), avroSerde))
        val witcherStream: KStream<String, Witcher> = witcherAvroStream.mapValues { witcherAvro ->
            val witcher = Witcher(
                witcherAvro["name"].toString(),
                witcherAvro["schoolName"].toString()
            )
            witcher
        }
        val resStream: KStream<String, String> = witcherStream.mapValues { witcher ->
            witcher.schoolName
        }
        resStream.to(schoolsTopic, Produced.with(Serdes.String(), Serdes.String()))

        val topology = streamsBuilder.build()
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["application.id"] = "kafka-1"
        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}