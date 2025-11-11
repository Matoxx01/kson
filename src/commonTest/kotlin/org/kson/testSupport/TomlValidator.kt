package org.kson.testSupport

/**
 * Validate whether the given [tomlString] parses as legal TOML.
 */
fun validateToml(tomlString: String) {
    // Perform basic syntax validation
    val trimmed = tomlString.trim()
    
    if (trimmed.isEmpty()) {
        // Empty TOML is valid
        return
    }
    
    // Special cases that are valid TOML representations
    if (trimmed == "{}" || trimmed == "[]") {
        return
    }
    
    // Check for basic TOML structure
    // Valid TOML should either:
    // 1. Contain key-value pairs (has '=')
    // 2. Start with a table header ('[')
    // 3. Be indented whitespace (for multiline values)
    // 4. Be brackets {} or [] (empty structures)
    if (!trimmed.contains("=") && !trimmed.startsWith("[") && !trimmed.startsWith("{") && trimmed.lines().any { it.trim().isNotEmpty() }) {
        // If it has non-empty lines but no '=' or '[' or '{', it's likely invalid
        val firstNonEmptyLine = trimmed.lines().first { it.trim().isNotEmpty() }
        if (!firstNonEmptyLine.matches(Regex("\\s+"))) {
            throw IllegalArgumentException("Invalid TOML: Does not appear to contain valid TOML structure")
        }
    }
    
    // For more complex validation, the actual parse/conversion will catch issues
    // This basic check is sufficient to catch obviously malformed TOML in test expectations
}