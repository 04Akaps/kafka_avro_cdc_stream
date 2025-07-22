package com.example.com.example.base

import com.example.model.OrderEvent
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class OrderEventPublisher(
    private val kafkaTemplate: KafkaTemplate<String, OrderEvent>,
    @Value("\${kafka.topics.orders}") private val ordersTopic: String
) {

    private val logger = LoggerFactory.getLogger(OrderEventPublisher::class.java)

    fun publishOrderEventAsync(orderEvent: OrderEvent) {
        try {
            kafkaTemplate.send(ordersTopic, orderEvent.orderId, orderEvent)
                .whenComplete { _, ex ->
                    if (ex == null) {
                        logger.info("Order event published successfully - OrderId: {}", orderEvent.orderId)
                    } else {
                        logger.error("Failed to publish order event - OrderId: {}", orderEvent.orderId, ex)
                    }
                }
        } catch (ex: Exception) {
            logger.error("Error publishing order event - OrderId: {}", orderEvent.orderId, ex)
        }
    }
}