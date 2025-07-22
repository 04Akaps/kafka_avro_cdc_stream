package com.example.avro

import org.apache.avro.Schema
import org.springframework.core.io.ClassPathResource
import org.springframework.stereotype.Component
import java.io.IOException

@Component
class OrderEventSchemaManager {
    
    private var cachedSchema: Schema? = null
    
    fun getOrderEventSchema(): Schema {
        return cachedSchema ?: loadSchemaFromFile("avro/order-entity.avsc").also {
            cachedSchema = it
        }
    }
    
    private fun loadSchemaFromFile(filePath: String): Schema {
        return try {
            val resource = ClassPathResource(filePath)
            val schemaContent = resource.inputStream.bufferedReader().use { it.readText() }
            Schema.Parser().parse(schemaContent)
        } catch (e: IOException) {
            throw IllegalStateException("Failed to load Avro schema from $filePath", e)
        }
    }

}