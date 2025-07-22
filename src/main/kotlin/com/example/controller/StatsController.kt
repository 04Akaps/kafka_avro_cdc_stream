package com.example.controller

import com.example.model.*
import com.example.com.example.stream.StatsQueryService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/stats")
@CrossOrigin(origins = ["*"])
class StatsController(
    private val statsQueryService: StatsQueryService
) {

    @GetMapping("/orders/count")
    fun getOrderCountStats(
        @RequestParam(defaultValue = "10") windowSizeSeconds: Long
    ): ResponseEntity<StatsResponse<OrderCountComparisonStats>> {
        val stats = statsQueryService.getOrderCountComparison()
        val response = StatsResponse(
            success = true,
            data = stats,
            message = "Order count statistics retrieved successfully"
        )
        return ResponseEntity.ok(response)
    }
}