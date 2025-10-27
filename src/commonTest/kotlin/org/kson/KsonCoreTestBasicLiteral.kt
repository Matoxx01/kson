package org.kson

import kotlin.test.Test

class KsonCoreTestBasicLiteral : KsonCoreTest {
    @Test
    fun testBooleanLiteralSource() {
        assertParsesTo(
            """
                true
            """,
            "true",
            "true",
            "true",
            "value = true"
        )

        assertParsesTo(
            """
                false
            """,
            "false",
            "false",
            "false",
            "value = false"
        )
    }

    @Test
    fun testNullLiteralSource() {
        assertParsesTo(
            """
                null
            """,
            "null",
            "null",
            "null",
            "value = \"null\""
        )
    }
} 
