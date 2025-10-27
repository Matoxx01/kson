package org.kson.testSupport

import com.akuleshov7.ktoml.Toml
import kotlinx.serialization.builtins.MapSerializer
import kotlinx.serialization.builtins.serializer

/**
 * Validate whether the given [tomlString] parses as legal TOML
 */
fun validateToml(tomlString: String) {
    try {
        val deserializer = MapSerializer(String.serializer(), String.serializer())
        Toml.decodeFromString(deserializer, tomlString)
    } catch (firstEx: Exception) {
        try {
            val trimmed = tomlString.trim()
            val wrapped = if (trimmed.startsWith("[") || trimmed.startsWith("{") ||
                trimmed.startsWith("\"") || trimmed.startsWith("'") ||
                !trimmed.contains("=")
            ) {
                // Quote the inner value if it's not already quoted to ensure it's parsed as a string value
                val inner = if (trimmed.startsWith("\"") || trimmed.startsWith("'")) trimmed else "\"${trimmed.replace("\"", "\\\"")}\""
                "value = $inner"
            } else {
                // if it already looks like a key=value pair, just retry
                tomlString
            }
            val deserializer = MapSerializer(String.serializer(), String.serializer())
            Toml.decodeFromString(deserializer, wrapped)
        } catch (secondEx: Exception) {
            throw IllegalArgumentException("Invalid TOML: ${firstEx.message}", firstEx)
        }
    }
}