package com.example.avro

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

@Component
class AvroOrderEventConsumer {
    
    private val logger = LoggerFactory.getLogger(AvroOrderEventConsumer::class.java)
    private val objectMapper = jacksonObjectMapper()
    
    @KafkaListener(topics = ["orders-avro"], groupId = "avro-order-processor-v1")
    fun handleOrderEventV1(
        @Payload jsonString: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int
    ) {
        try {
            val orderData = extractOrderDataFromJson(jsonString)
            
            logger.info("Processing Avro order v1: {} from partition {}", 
                orderData.orderId, partition)
            
            processOrderData(orderData)
            
        } catch (ex: Exception) {
            logger.error("Failed to process Avro order event v1", ex)
        }
    }

    
    private fun extractOrderDataFromJson(jsonString: String): OrderDataV1 {
        val jsonMap: Map<String, Any> = objectMapper.readValue(jsonString)
        
        return OrderDataV1(
            orderId = jsonMap["orderId"].toString(),
            customerId = jsonMap["customerId"].toString(),
            quantity = jsonMap["quantity"] as Int,
            price = BigDecimal(jsonMap["price"].toString()),
            timestamp = convertTimestamp(jsonMap["createdAt"] as Long),
            status = jsonMap["status"].toString(),
            version = (jsonMap["version"] as Int).toLong()
        )
    }

    private fun convertTimestamp(epochMilli: Long): LocalDateTime {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneOffset.UTC)
    }
    
    private fun processOrderData(orderData: OrderDataV1) {
        logger.info("Processing order: {} for customer: {}, amount: {}", 
            orderData.orderId, orderData.customerId, orderData.price)
        
        validateBasicOrder(orderData)
        updateInventory(orderData)
    }

    private fun validateBasicOrder(orderData: OrderDataV1) {
        logger.debug("Validating basic order: {}", orderData.orderId)
    }
    
    private fun updateInventory(orderData: OrderDataV1) {
        logger.debug("Updating inventory for order: {}", orderData.orderId)
    }

}

data class OrderDataV1(
    val orderId: String,
    val customerId: String,
    val quantity: Int,
    val price: BigDecimal,
    val timestamp: LocalDateTime,
    val status: String,
    val version: Long
)