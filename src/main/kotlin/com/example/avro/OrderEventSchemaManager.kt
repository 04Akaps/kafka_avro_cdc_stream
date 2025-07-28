package com.example.avro

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.Schema
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.ClassPathResource
import org.springframework.stereotype.Component
import java.io.IOException

@Component
class OrderEventSchemaManager(
    @Value("\${schema.registry.url:http://localhost:8081}")
    private val schemaRegistryUrl: String
) {
    
    private val logger = LoggerFactory.getLogger(OrderEventSchemaManager::class.java)
    private val schemaRegistryClient: SchemaRegistryClient = CachedSchemaRegistryClient(schemaRegistryUrl, 100)
    private var cachedSchema: Schema? = null
    
    fun getOrderEventSchema(): Schema {
        return cachedSchema ?: loadSchemaFromRegistry().also {
            cachedSchema = it
        }
    }
    
    private fun loadSchemaFromRegistry(): Schema {
        return try {
            val subject = "orders-avro-value"
            logger.info("Loading schema from registry for subject: {}", subject)
            
            val latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject)
            val schemaString = latestSchemaMetadata.schema
            
            logger.info("Successfully loaded schema from registry. Version: {}, ID: {}", 
                latestSchemaMetadata.version, latestSchemaMetadata.id)
            
            Schema.Parser().parse(schemaString)
            
        } catch (e: Exception) {
            logger.warn("Failed to load schema from registry, falling back to local file: {}", e.message)
            loadSchemaFromFile("avro/order-entity.avsc")
        }
    }
    
    private fun loadSchemaFromFile(filePath: String): Schema {
        return try {
            logger.info("Loading schema from local file: {}", filePath)
            val resource = ClassPathResource(filePath)
            val schemaContent = resource.inputStream.bufferedReader().use { it.readText() }
            Schema.Parser().parse(schemaContent)
        } catch (e: IOException) {
            throw IllegalStateException("Failed to load Avro schema from $filePath", e)
        }
    }
    
    fun registerSchema(): Int {
        return try {
            val subject = "orders-avro-value"
            val schema = loadSchemaFromFile("avro/order-entity.avsc")
            val schemaId = schemaRegistryClient.register(subject, schema)
            logger.info("Successfully registered schema with ID: {} for subject: {}", schemaId, subject)
            schemaId
        } catch (e: Exception) {
            logger.error("Failed to register schema", e)
            throw e
        }
    }

}