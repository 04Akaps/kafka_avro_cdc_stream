package com.example.avro

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.LocalDateTime
import java.time.ZoneOffset

@Service
class AvroOrderEventProducer(
    private val avroKafkaTemplate: KafkaTemplate<String, String>
) {
    
    private val logger = LoggerFactory.getLogger(AvroOrderEventProducer::class.java)
    private val objectMapper = jacksonObjectMapper()
    
    fun publishOrderEvent(
        orderId: String,
        customerId: String,
        quantity: Int,
        price: BigDecimal,
        schema: Schema
    ) {
        try {
            val avroRecord = createAvroRecord(orderId, customerId, quantity, price, schema)
            val jsonString = convertRecordToJson(avroRecord)
            
            avroKafkaTemplate.send("orders-avro", orderId, jsonString)
                .whenComplete { result, ex ->
                    if (ex == null) {
                        logger.info("Avro order event published: {}", orderId)
                    } else {
                        logger.error("Failed to publish Avro order event: {}", orderId, ex)
                    }
                }
                
        } catch (ex: Exception) {
            logger.error("Error creating Avro record for order: {}", orderId, ex)
        }
    }
    
    private fun createAvroRecord(
        orderId: String,
        customerId: String,
        quantity: Int,
        price: BigDecimal,
        schema: Schema
    ): GenericRecord {
        val record = GenericData.Record(schema)
        
        val now = LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()
        
        record.put("orderId", orderId)
        record.put("customerId", customerId)
        record.put("quantity", quantity)
        record.put("price", convertPriceToBytes(price))
        record.put("status", "PENDING")
        record.put("createdAt", now)
        record.put("updatedAt", now)
        record.put("version", 1L)
        
        return record
    }
    
    private fun convertPriceToBytes(price: BigDecimal): ByteBuffer {
        val scaled = price.setScale(2)
        val unscaledValue = scaled.unscaledValue()
        return ByteBuffer.wrap(unscaledValue.toByteArray())
    }
    
    private fun convertRecordToJson(record: GenericRecord): String {
        val map = mutableMapOf<String, Any?>()
        
        for (field in record.schema.fields) {
            val value = record.get(field.name())
            map[field.name()] = when (value) {
                is ByteBuffer -> {
                    val bytes = ByteArray(value.remaining())
                    value.get(bytes)
                    val bigInt = java.math.BigInteger(bytes)
                    BigDecimal(bigInt, 2).toString()
                }
                else -> value
            }
        }
        
        return objectMapper.writeValueAsString(map)
    }
}