package com.example.com.example.stream

import com.example.model.OrderCountComparisonStats
import com.example.model.PeriodStats
import com.example.model.WindowedOrderCount
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyWindowStore
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.stereotype.Service
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

@Service
class StatsQueryService(
    private val streamsBuilderFactoryBean: StreamsBuilderFactoryBean
) {
    
    fun getOrderCountComparison(): OrderCountComparisonStats? {
        return try {
            val streams = streamsBuilderFactoryBean.kafkaStreams
            if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
                null
            } else {
                val store: ReadOnlyWindowStore<String, WindowedOrderCount> = streams.store(
                    StoreQueryParameters.fromNameAndType("order-count-store", QueryableStoreTypes.windowStore())
                )
                
                val now = Instant.now()
                val currentPeriodEnd = now
                val currentPeriodStart = now.minusSeconds(300) // 5분 전
                val previousPeriodEnd = currentPeriodStart
                val previousPeriodStart = currentPeriodStart.minusSeconds(300) // 10분 전
                
                val currentCount = getCountForPeriod(store, currentPeriodStart, currentPeriodEnd)
                val previousCount = getCountForPeriod(store, previousPeriodStart, previousPeriodEnd)
                
                val changeCount = currentCount - previousCount
                val changePercentage = if (previousCount > 0) {
                    (changeCount.toDouble() / previousCount.toDouble()) * 100.0
                } else if (currentCount > 0) {
                    100.0
                } else {
                    0.0
                }
                
                OrderCountComparisonStats(
                    currentPeriod = PeriodStats(
                        windowStart = LocalDateTime.ofInstant(currentPeriodStart, ZoneOffset.UTC),
                        windowEnd = LocalDateTime.ofInstant(currentPeriodEnd, ZoneOffset.UTC),
                        orderCount = currentCount
                    ),
                    previousPeriod = PeriodStats(
                        windowStart = LocalDateTime.ofInstant(previousPeriodStart, ZoneOffset.UTC),
                        windowEnd = LocalDateTime.ofInstant(previousPeriodEnd, ZoneOffset.UTC),
                        orderCount = previousCount
                    ),
                    changeCount = changeCount,
                    changePercentage = changePercentage,
                    isIncreasing = changeCount > 0
                )
            }
        } catch (e: Exception) {
            null
        }
    }
    
    private fun getCountForPeriod(
        store: ReadOnlyWindowStore<String, WindowedOrderCount>,
        startTime: Instant,
        endTime: Instant
    ): Long {
        var totalCount = 0L
        store.fetchAll(startTime, endTime).use { iterator ->
            while (iterator.hasNext()) {
                val entry = iterator.next()
                totalCount += entry.value.count
            }
        }
        return totalCount
    }
}