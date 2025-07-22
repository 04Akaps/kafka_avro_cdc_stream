package com.example.cdc

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

@Component
class OrderCdcEventProcessor(
    private val objectMapper: ObjectMapper
) {
    
    private val logger = LoggerFactory.getLogger(OrderCdcEventProcessor::class.java)
    
    @KafkaListener(
        topics = ["dbserver1.public.orders"],
        groupId = "real-order-cdc-processor",
        containerFactory = "cdcKafkaListenerContainerFactory"
    )
    fun processRealCdcEvent(@Payload cdcMessage: String) {
        try {
            logger.info("Raw CDC message: {}", cdcMessage)
            val cdcEvent = objectMapper.readTree(cdcMessage)

            logger.info("Received CDC event: processing transformed message")

            handleTransformedEvent(cdcEvent)
            
        } catch (ex: Exception) {
            logger.error("Failed to process CDC event: {}", cdcMessage, ex)
        }
    }
    
    private fun handleTransformedEvent(cdcEvent: JsonNode) {
        val orderData = parseOrderData(cdcEvent)
        
        logger.info("Processing order event: {}", orderData.orderId)

        handleNewOrderLogic(orderData)
    }

    
    private fun parseOrderData(node: JsonNode): OrderChangeData {
        return OrderChangeData(
            orderId = node.get("order_id")?.asText() ?: "",
            customerId = node.get("customer_id")?.asText() ?: "",
            quantity = node.get("quantity")?.asInt() ?: 0,
            price = node.get("price")?.asText()?.let { BigDecimal(it) } ?: BigDecimal.ZERO,
            status = node.get("status")?.asText()?.let { OrderStatus.valueOf(it) } ?: OrderStatus.PENDING,
            createdAt = parseTimestamp(node.get("created_at")?.asText()),
            updatedAt = parseTimestamp(node.get("updated_at")?.asText()),
            version = node.get("version")?.asLong() ?: 0
        )
    }
    

    private fun detectAndHandleChanges(before: OrderChangeData, after: OrderChangeData) {

        if (before.status != after.status) {
            handleStatusChange(before.status, after.status, after.orderId)
        }
        

        if (before.quantity != after.quantity) {
            handleQuantityChange(before.quantity, after.quantity, after.orderId)
        }
        
        // 가격 변경 감지
        if (before.price.compareTo(after.price) != 0) {
            handlePriceChange(before.price, after.price, after.orderId)
        }
    }
    
    private fun handleStatusChange(oldStatus: OrderStatus, newStatus: OrderStatus, orderId: String) {
        logger.info("Order {} status changed: {} -> {}", orderId, oldStatus, newStatus)
        
        when (newStatus) {
            OrderStatus.CONFIRMED -> handleOrderConfirmation(orderId)
            OrderStatus.SHIPPED -> handleOrderShipment(orderId)
            OrderStatus.DELIVERED -> handleOrderDelivery(orderId)
            OrderStatus.CANCELLED -> handleOrderCancellation(orderId)
            else -> { }
        }
    }
    
    private fun handleNewOrderLogic(orderData: OrderChangeData) {
        logger.debug("Executing new order logic for: {}", orderData.orderId)
    }
    
    private fun handleOrderDeletionLogic(orderData: OrderChangeData) {
        logger.debug("Executing order deletion logic for: {}", orderData.orderId)
    }
    
    private fun handleQuantityChange(oldQty: Int, newQty: Int, orderId: String) {
        logger.info("Order {} quantity changed: {} -> {}", orderId, oldQty, newQty)
    }
    
    private fun handlePriceChange(oldPrice: BigDecimal, newPrice: BigDecimal, orderId: String) {
        logger.info("Order {} price changed: {} -> {}", orderId, oldPrice, newPrice)
    }
    
    private fun handleOrderConfirmation(orderId: String) {
        logger.info("Processing order confirmation: {}", orderId)
    }
    
    private fun handleOrderShipment(orderId: String) {
        logger.info("Processing order shipment: {}", orderId)
    }
    
    private fun handleOrderDelivery(orderId: String) {
        logger.info("Processing order delivery: {}", orderId)
    }
    
    private fun handleOrderCancellation(orderId: String) {
        logger.info("Processing order cancellation: {}", orderId)
    }
    
    private fun handleSnapshotData(orderData: OrderChangeData) {
        logger.debug("Processing snapshot data for order: {}", orderData.orderId)
    }
    
    private fun parseTimestamp(timestamp: String?): LocalDateTime? {
        return timestamp?.let {
            try {
                        val epochMicros = it.toLong()
                val epochMillis = epochMicros / 1000
                java.time.Instant.ofEpochMilli(epochMillis).atZone(java.time.ZoneId.systemDefault()).toLocalDateTime()
            } catch (ex: Exception) {
                logger.warn("Failed to parse timestamp: {}", it)
                null
            }
        }
    }
}

data class OrderChangeData(
    val orderId: String,
    val customerId: String,
    val quantity: Int,
    val price: BigDecimal,
    val status: OrderStatus,
    val createdAt: LocalDateTime?,
    val updatedAt: LocalDateTime?,
    val version: Long
)