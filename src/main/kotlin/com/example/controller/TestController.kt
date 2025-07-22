package com.example.controller

import com.example.avro.AvroOrderEventProducer
import com.example.cdc.OrderRepository
import com.example.cdc.OrderStatus
import com.example.cdc.OrderCdcService
import com.example.com.example.model.CreateOrderRequest
import com.example.com.example.base.OrderEventPublisher
import com.example.model.OrderEvent
import com.example.avro.OrderEventSchemaManager
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.math.BigDecimal
import java.util.*

@RestController
@RequestMapping("/api/test")
class TestController(
    private val orderEventPublisher: OrderEventPublisher,
    private val avroOrderEventProducer: AvroOrderEventProducer,
    private val orderRepository: OrderRepository,
    private val schemaManager: OrderEventSchemaManager,
    private val orderCdcService: OrderCdcService
) {

    @PostMapping
    fun createOrder(@RequestBody createOrderRequest: CreateOrderRequest): ResponseEntity<String> {
        val orderEvent = OrderEvent(
            orderId = UUID.randomUUID().toString(),
            customerId = createOrderRequest.customerId,
            quantity = createOrderRequest.quantity,
            price = createOrderRequest.price,
        )

        orderEventPublisher.publishOrderEventAsync(orderEvent)

        return ResponseEntity.ok("event published: ${orderEvent.orderId}")
    }


    @PostMapping("/avro/publish")
    fun publishAvroEvent(@RequestBody request: TestOrderRequest): ResponseEntity<String> {
        avroOrderEventProducer.publishOrderEvent(
            orderId = request.orderId,
            customerId = request.customerId,
            quantity = request.quantity,
            price = request.price,
            schema = schemaManager.getOrderEventSchema()
        )
        
        return ResponseEntity.ok("Avro event published: ${request.orderId}")
    }
    
    @PostMapping("/cdc/create")
    fun createOrderForCdc(@RequestBody request: TestOrderRequest): ResponseEntity<String> {
        val savedOrder = orderCdcService.createOrder(
            orderId = request.orderId,
            customerId = request.customerId,
            quantity = request.quantity,
            price = request.price
        )
        
        return ResponseEntity.ok("Order created in DB (CDC will trigger): ${savedOrder.orderId}")
    }
    
    @PutMapping("/cdc/update/{orderId}")
    fun updateOrderForCdc(
        @PathVariable orderId: String,
        @RequestParam status: OrderStatus
    ): ResponseEntity<String> {
        val updatedOrder = orderCdcService.updateOrderStatus(orderId, status)
        val updated = updatedOrder != null
        
        return if (updated) {
            ResponseEntity.ok("Order updated in DB (CDC will trigger): $orderId -> $status")
        } else {
            ResponseEntity.notFound().build()
        }
    }
    
    @DeleteMapping("/cdc/delete/{orderId}")
    fun deleteOrderForCdc(@PathVariable orderId: String): ResponseEntity<String> {
        val deleted = orderCdcService.deleteOrder(orderId)
        
        return if (deleted) {
            ResponseEntity.ok("Order deleted from DB (CDC will trigger): $orderId")
        } else {
            ResponseEntity.notFound().build()
        }
    }

    @GetMapping("/orders")
    fun getAllOrders(): ResponseEntity<List<Any>> {
        val orders = orderRepository.findAll()
        return ResponseEntity.ok(orders)
    }
    
    @GetMapping("/orders/{orderId}")
    fun getOrder(@PathVariable orderId: String): ResponseEntity<Any> {
        val order = orderRepository.findById(orderId)
        return if (order.isPresent) {
            ResponseEntity.ok(order.get())
        } else {
            ResponseEntity.notFound().build()
        }
    }

}

data class TestOrderRequest(
    val orderId: String,
    val customerId: String,
    val quantity: Int,
    val price: BigDecimal,
    val status: OrderStatus = OrderStatus.PENDING
)

