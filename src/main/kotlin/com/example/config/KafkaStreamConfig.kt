package com.example.config

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.config.KafkaStreamsConfiguration

@Configuration
@EnableKafka
@EnableKafkaStreams
class KafkaStreamConfig(private val kafkaProperties: KafkaProperties) {

    @Bean("defaultKafkaStreamsConfig")
    fun defaultKafkaStreamsConfig(): KafkaStreamsConfiguration {
        val props = kafkaProperties.streams.buildProperties(null)

        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java

        props["rack.aware.assignment.strategy"] = "none"
        
        return KafkaStreamsConfiguration(props)
    }
}