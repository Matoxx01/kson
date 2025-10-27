package org.kson

import kotlin.test.Test

class KsonCoreTestList : KsonCoreTest {
    @Test
    fun testEmptyListSource() {
        assertParsesTo(
            """
                []
            """,
            "<>",
            "[]",
            "[]",
            "value = []"
        )
    }

    @Test
    fun testSquareBracketListSource() {
        assertParsesTo(
            """
                ["a string"]
            """,
            """
                - 'a string'
            """.trimIndent(),
            """
                - "a string"
            """.trimIndent(),
            """
                [
                  "a string"
                ]
            """.trimIndent(),
      expectedToml = "value = [\"a string\"]"
        )

        assertParsesTo(
            """
                [42.4, 43.1, 44.7]
            """,
            """
                - 42.4
                - 43.1
                - 44.7
            """.trimIndent(),
            """
                - 42.4
                - 43.1
                - 44.7
            """.trimIndent(),
            """
                [
                  42.4,
                  43.1,
                  44.7
                ]
            """.trimIndent(),
      expectedToml = "value = [42.4, 43.1, 44.7]"
        )

        assertParsesTo(
            """
                [true, false, null,]
            """,
            """
                - true
                - false
                - null
            """.trimIndent(),
            """
                - true
                - false
                - null
            """.trimIndent(),
            """
                [
                  true,
                  false,
                  null
                ]
            """.trimIndent(),
            expectedToml = "value = [true, false, \"null\"]",
            message = "should support an optional trailing comma in lists"
        )

        assertParsesTo(
            """
                [true, false, [1.2, 3.4, 5.6]]
            """,
            """
                - true
                - false
                - 
                  - 1.2
                  - 3.4
                  - 5.6
            """.trimIndent(),
            """
                - true
                - false
                - 
                  - 1.2
                  - 3.4
                  - 5.6
            """.trimIndent(),
            """
                [
                  true,
                  false,
                  [
                    1.2,
                    3.4,
                    5.6
                  ]
                ]
            """.trimIndent(),
      "value = [true, false, [1.2, 3.4, 5.6]]"
        )
    }

    @Test
    fun testDashListSource() {
        assertParsesTo(
            """
                - "a string"
            """,
            """
                - 'a string'
            """.trimIndent(),
            """
                - "a string"
            """.trimIndent(),
            """
                [
                  "a string"
                ]
            """.trimIndent(),
      "value = [\"a string\"]"
        )

        assertParsesTo(
            """
                - 42.4
                - 43.1
                - 44.7
            """,
            """
                - 42.4
                - 43.1
                - 44.7
            """.trimIndent(),
            """
                - 42.4
                - 43.1
                - 44.7
            """.trimIndent(),
            """
                [
                  42.4,
                  43.1,
                  44.7
                ]
            """.trimIndent(),
      "value = [42.4, 43.1, 44.7]"
        )
    }

    @Test
    fun testDelimitedDashList() {
        assertParsesTo("""
                <>
            """.trimIndent(),
            "<>",
      "[]",
      "[]",
      "value = []"
        )

        assertParsesTo("""
                < - a - b - c >
            """.trimIndent(),
            """
                - a
                - b
                - c
            """.trimIndent(),
            """
                - a
                - b
                - c
            """.trimIndent(),
            """
                [
                  "a",
                  "b",
                  "c"
                ]
            """.trimIndent(),
      "value = [\"a\", \"b\", \"c\"]"
        )

        assertParsesTo("""
                < 
                  - a 
                  - b 
                  - c 
                >
            """.trimIndent(),
            """
                - a
                - b
                - c
            """.trimIndent(),
            """
                - a
                - b
                - c
            """.trimIndent(),
            """
                [
                  "a",
                  "b",
                  "c"
                ]
            """.trimIndent(),
      "value = [\"a\", \"b\", \"c\"]"
        )
    }

    @Test
    fun testDashListNestedWithCommaList() {
        assertParsesTo("""
            [- []]
        """.trimIndent(),
            """
               - 
                 - <>
            """.trimIndent(),
            """
               - 
                 - []
            """.trimIndent(),
            """
               [
                 [
                   []
                 ]
               ]
            """.trimIndent(),
      "value = [[[]]]"
        )
    }

    @Test
    fun testDashListNestedWithObject() {
        assertParsesTo("""
            - { 
                nestedDashList: - a
                                - b
                                - c
              }
        """.trimIndent(),
            """
                - nestedDashList:
                    - a
                    - b
                    - c
            """.trimIndent(),
            """
                - nestedDashList:
                    - a
                    - b
                    - c
            """.trimIndent(),
            """
                [
                  {
                    "nestedDashList": [
                      "a",
                      "b",
                      "c"
                    ]
                  }
                ]
            """.trimIndent(),
      "value = [{nestedDashList = [\"a\", \"b\", \"c\"]}]"
        )
    }

    @Test
    fun testDashListNestedWithDashList() {
        assertParsesTo("""
            - <
                - a
                - b
                - <
                    - a1
                    - b1
                    - c1
                  >
                - c
              >
        """.trimIndent(),
            """
                - 
                  - a
                  - b
                  - 
                    - a1
                    - b1
                    - c1
                    =
                  - c
            """.trimIndent(),
            """
                - 
                  - a
                  - b
                  - 
                    - a1
                    - b1
                    - c1
                  - c
            """.trimIndent(),
            """
                [
                  [
                    "a",
                    "b",
                    [
                      "a1",
                      "b1",
                      "c1"
                    ],
                    "c"
                  ]
                ]
            """.trimIndent(),
      "value = [[\"a\", \"b\", [\"a1\", \"b1\", \"c1\"], \"c\"]]"
        )
    }

    @Test
    fun testCommaFreeList() {
        assertParsesTo("""
            [
                null true [sublist] 
                - another 
                - sublist
            ]
        """,
            """
            - null
            - true
            - 
              - sublist
              =
            - 
              - another
              - sublist
        """.trimIndent(),
            """
            - null
            - true
            - 
              - sublist
            - 
              - another
              - sublist
        """.trimIndent(),
            """
            [
              null,
              true,
              [
                "sublist"
              ],
              [
                "another",
                "sublist"
              ]
            ]
        """.trimIndent(),
            """
              value = [
                null,
                true,
                [
                  "sublist"
                ],
                [
                  "another",
                  "sublist"
                ]
              ]
              """.trimIndent()
        )
    }

    @Test
    fun testNestedNonDelimitedDashLists() {
        assertParsesTo(
            """
                - 
                  - "sub-list elem 1"
                  - "sub-list elem 2"
                  =
                - "outer list elem 1"
            """.trimIndent(),
            """
                - 
                  - 'sub-list elem 1'
                  - 'sub-list elem 2'
                  =
                - 'outer list elem 1'
            """.trimIndent(),
            """
                - 
                  - "sub-list elem 1"
                  - "sub-list elem 2"
                - "outer list elem 1"
            """.trimIndent(),
            """
                [
                  [
                    "sub-list elem 1",
                    "sub-list elem 2"
                  ],
                  "outer list elem 1"
                ]
            """.trimIndent(),
      "value = [[\"sub-list elem 1\", \"sub-list elem 2\"], \"outer list elem 1\"]"
        )
    }
} 
