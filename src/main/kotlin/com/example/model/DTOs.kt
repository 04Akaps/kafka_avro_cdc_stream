package com.example.com.example.model

import java.math.BigDecimal

data class CreateOrderRequest(
    val customerId: String,
    val quantity: Int,
    val price: BigDecimal
)