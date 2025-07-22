package com.example.com.example.model

import java.math.BigDecimal

data class CreateOrderRequest(
    val customerId: String,
    val quantity: Int,
    val price: BigDecimal
)

data class OrderResponse(
    val orderId: String,
    val customerId: String,
    val quantity: Int,
    val price: BigDecimal,
    val message: String? = null
)

