package sample.dyn.model

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import java.util.UUID

data class Hotel(
    val id: String = UUID.randomUUID().toString(),
    val name: String,
    val address: String? = null,
    val state: String? = null,
    val zip: String? = null,
    val version: Long = 1L,
    val properties: JsonNode = JsonNodeFactory.instance.objectNode()
)