package com.example

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams

@SpringBootApplication
@EnableKafka
@EnableKafkaStreams
class OrderProcessingApplication

fun main(args: Array<String>) {
    runApplication<OrderProcessingApplication>(*args)
}