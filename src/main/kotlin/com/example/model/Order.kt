package com.example.com.example.model

import java.math.BigDecimal
import java.time.LocalDateTime

data class Order(
    val orderId: String,
    val customerId: String,
    val quantity: Int,
    val price: BigDecimal,
    val createdAt: LocalDateTime = LocalDateTime.now(),
    val updatedAt: LocalDateTime = LocalDateTime.now()
)