package org.kson.ast

import org.kson.CompileTarget
import org.kson.CompileTarget.*
import org.kson.ast.AstNode.Indent
import org.kson.parser.Location
import org.kson.parser.behavior.embedblock.EmbedDelim
import org.kson.parser.NumberParser
import org.kson.parser.NumberParser.ParsedNumber
import org.kson.tools.IndentType
import org.kson.tools.FormattingStyle
import org.kson.parser.Parser
import org.kson.parser.behavior.StringQuote
import org.kson.parser.behavior.StringQuote.*
import org.kson.parser.behavior.StringUnquoted
import org.kson.parser.behavior.embedblock.EmbedObjectKeys

interface AstNode {
    /**
     * Public method for transforming the AST rooted at this node into the source of the given [compileTarget],
     * rendered with the given [indent]
     */
    fun toSource(indent: Indent, compileTarget: CompileTarget): String = toSourceWithNext(indent, null, compileTarget)

    /**
     * Internal method for recursive source generation calls must pass down context about the next node to be
     * rendered from this tree.
     *
     * This should only be called by other [AstNode] implementations.
     */
    fun toSourceWithNext(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String

    /**
     * The source location from which this [AstNode] was parsed
     */
    val location: Location

    /**
     * Abstract representation of the indentation to apply when serializing an AST as source code
     */
    data class Indent(
        /**
         * The [IndentType] to use when indenting output source
         */
        private val indentType: IndentType,
        /**
         * How deep to make this indent
         */
        private val indentLevel: Int = 0,
        /**
         * Whether or not this indent "hangs", i.e. only starts after the first newline of the text being indented
         */
        val hangingIndent: Boolean = false
    ) {
        /**
         * Constructs an initial/default indent
         */
        constructor() : this(IndentType.Space(2), 0, false)

        private val indentString = indentType.indentString

        fun firstLineIndent(): String {
            return if (hangingIndent) {
                ""
            } else {
                return bodyLinesIndent()
            }
        }

        fun bodyLinesIndent(): String {
            return indentString.repeat(indentLevel)
        }

        /**
         * Produce a copy of this indent with the given [hanging] value for its [hanging]
         */
        fun clone(hanging: Boolean): Indent {
            return Indent(indentType, indentLevel, hanging)
        }

        /**
         * Produce the "next" indent in from this one, with the given [hanging] value for its [hanging]
         */
        fun next(hanging: Boolean): Indent {
            return Indent(indentType, indentLevel + 1, hanging)
        }
    }
}

/**
 * Base [AstNode] to be subclassed by all Kson AST Node classes
 */
sealed class AstNodeImpl(override val location: Location) : AstNode {
    /**
     * Transpiles this [AstNode] to the given [compileTarget] source, respecting the configuration in the given
     * [CompileTarget]
     */
    override fun toSourceWithNext(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        return if (compileTarget.preserveComments && this is Documented && comments.isNotEmpty()) {
            // if we have comments, write them followed by the node content on the next line with an appropriate indent
            indent.firstLineIndent() + comments.joinToString("\n${indent.bodyLinesIndent()}") +
                    "\n" + toSourceInternal(indent.clone(false), nextNode, compileTarget)
        } else {
            // otherwise, just pass through to the node content
            toSourceInternal(indent, nextNode, compileTarget)
        }
    }

    /**
     * Subclasses must implement serialization of the AST subtree rooted at their node to a corresponding
     * source code snippet for [compileTarget], EXCLUDING comments (comment writing is handled "higher" up
     * in [toSourceWithNext]).
     *
     * This method is protected since it should never be called outside of [toSourceWithNext], which handles ensuring
     * comments are properly serialized for all nodes when appropriate.  So:
     *
     * DO NOT call this method---call [toSourceWithNext] instead.
     */
    protected abstract fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String
}
 
/**
 * Base class for the "shadow" versions of some of our [AstNode]s that we create to stitch into a partial
 * AST built out of some source with errors.
 *
 * All the subclasses of this use the same strategy of having an interface define the node type and providing
 * two implementations: the concrete `Impl` version for valid [AstNode]s and the "shadow" `Error` implementation
 * which patches the AST with an [AstNodeError] where an [AstNodeImpl] would otherwise go
 */
open class AstNodeError(private val invalidSource: String, location: Location) : AstNode, AstNodeImpl(location) {
    override fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        return when (compileTarget) {
            is Kson, is Yaml, is Json, is Toml -> {
                invalidSource.split("\n")
                    .joinToString("\n") { line ->
                        indent.firstLineIndent() + line
                    }
            }
        }
    }
}

/**
 * Core Ast type for the values expressible in [Kson].  This type that maps to the `ksonValue` element of
 * the grammar documented on [Parser]
 */
interface KsonValueNode : AstNode
class KsonValueNodeError(content: String, location: Location) : KsonValueNode, AstNodeError(content, location)
abstract class KsonValueNodeImpl(location: Location) : KsonValueNode, AstNodeImpl(location)

/**
 * Any kson entity is either the [KsonRoot] of the document, an [ObjectPropertyNode]
 * on an object, or a [ListElementNode] in a list, and so semantically, those are the things
 * that make sense to document, so in our comment preservation strategy, these are the
 * [AstNode]s which accept comments.  This interface ties them together.
 */
interface Documented {
    val comments: List<String>
}

interface KsonRoot : AstNode
class KsonRootError(content: String, location: Location) : KsonRoot, AstNodeError(content, location)
class KsonRootImpl(
    val rootNode: KsonValueNode,
    private val trailingContent: List<KsonValueNode>,
    override val comments: List<String>,
    private val documentEndComments: List<String>,
    location: Location
) : KsonRoot, AstNodeImpl(location), Documented {

    /**
     * Produces valid [compileTarget] source code for the AST rooted at this [KsonRoot]
     */
    override fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        return when (compileTarget) {
            is Kson, is Yaml, is Json, is Toml -> {
                var ksonDocument = rootNode.toSourceWithNext(indent, null, compileTarget)

                trailingContent.forEach {
                    if (ksonDocument.takeLast(2) != "\n\n") {
                        ksonDocument += "\n\n"
                    }
                    ksonDocument += it.toSourceWithNext(indent, null, compileTarget)
                }

                // remove any trailing newlines
                while (ksonDocument.endsWith("\n")) {
                    ksonDocument = ksonDocument.removeSuffix("\n")
                }

                // TOML: Handle wrapping and comments for root nodes
                if (compileTarget is Toml) {
                    when (rootNode) {
                        is ObjectNode -> {
                            // Objects don't need wrapping, just add end comments
                            if (compileTarget.preserveComments && documentEndComments.isNotEmpty()) {
                                val endComments = documentEndComments.joinToString("\n") { it.trimEnd() }
                                ksonDocument += if (ksonDocument.endsWith(endComments)) "" else "\n\n" + endComments
                            }
                        }
                        is ListNode -> {
                            // Lists always need value = wrapping in TOML
                            val listNode = rootNode
                            val elementsWithComments = listNode.elements.filterIsInstance<ListElementNodeImpl>()
                                .filter { it.comments.isNotEmpty() }
                            
                            if (elementsWithComments.size == 1 && compileTarget.preserveComments) {
                                // Single element with comments - put comments before value =
                                val elementComments =
                                    elementsWithComments.first().comments.joinToString("\n") { it.trimEnd() }
                                ksonDocument = elementComments + "\n" + "value = " + ksonDocument.trim()
                            } else if (elementsWithComments.size > 1 && compileTarget.preserveComments) {
                                // Multiple elements with comments - already formatted as separate value = lines
                                // Don't add another value = wrapper
                            } else {
                                // Regular list - wrap with value =
                                ksonDocument = "value = " + ksonDocument.trim()
                            }
                            
                            // Add end comments if any
                            if (compileTarget.preserveComments && documentEndComments.isNotEmpty()) {
                                val endComments = documentEndComments.joinToString("\n") { it.trimEnd() }
                                ksonDocument += if (ksonDocument.endsWith(endComments)) "" else "\n\n" + endComments
                            }
                        }
                        else -> {
                            // Scalars (strings, numbers, booleans, null)
                            // Embed blocks are special:
                            // - With retainEmbedTags: they become inline tables (objects), don't wrap
                            // - Without retainEmbedTags AND with comments: wrap with value = (like scalars)
                            // - Without retainEmbedTags AND without comments: don't wrap (already properly formatted)
                            val isEmbedBlock = rootNode is EmbedBlockNode
                            
                            if (!isEmbedBlock) {
                                // For TOML, regular scalars get wrapped with value =
                                val hasDocEndComments = compileTarget.preserveComments && documentEndComments.isNotEmpty()

                                if (hasDocEndComments) {
                                    // Put document end comments BEFORE value =
                                    val endComments = documentEndComments.joinToString("\n") { it.trimEnd() }
                                    val scalarValue = ksonDocument.trimStart()
                                    ksonDocument = "$endComments\nvalue = $scalarValue"
                                } else {
                                    // Regular scalar - wrap with value =
                                    val scalarValue = ksonDocument.trimStart()
                                    ksonDocument = "value = $scalarValue"
                                }
                            } else {
                                // Embed blocks should emit their own properties (embedContent/embedTag/etc.)
                                // and must not be wrapped with "value =". Just add end comments if any.
                                if (compileTarget.preserveComments && documentEndComments.isNotEmpty()) {
                                    val endComments = documentEndComments.joinToString("\n")
                                    ksonDocument += if (ksonDocument.endsWith(endComments)) "" else "\n\n" + endComments
                                }
                            }
                        }
                    }
                } else {
                    // For non-TOML targets, add document end comments at the end as usual
                    if (compileTarget.preserveComments && documentEndComments.isNotEmpty()) {
                        val endComments = documentEndComments.joinToString("\n")
                        ksonDocument += if (ksonDocument.endsWith(endComments)) {
                            ""
                        } else {
                            if (compileTarget is Kson && compileTarget.formatConfig.formattingStyle == FormattingStyle.COMPACT) {
                                "\n" + endComments
                            } else {
                                "\n\n" + endComments
                            }
                        }
                    }
                }

                ksonDocument
            }
        }
    }
}

class ObjectNode(val properties: List<ObjectPropertyNode>, location: Location) : KsonValueNodeImpl(location) {
    override fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        if (properties.isEmpty()) {
            return "${indent.firstLineIndent()}{}"
        }

        return when (compileTarget) {
            is Kson -> {
                when (compileTarget.formatConfig.formattingStyle) {
                    FormattingStyle.DELIMITED -> formatDelimitedObject(indent, nextNode, compileTarget)
                    FormattingStyle.PLAIN -> formatUndelimitedObject(indent, nextNode, compileTarget)
                    FormattingStyle.COMPACT -> formatCompactObject(indent, nextNode, compileTarget)
                }
            }

            is Yaml -> formatUndelimitedObject(indent, nextNode, compileTarget)
            is Toml -> formatTomlObject(indent, nextNode, compileTarget)
            is Json -> formatDelimitedObject(indent, nextNode, compileTarget)
        }
    }

    private fun formatDelimitedObject(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        val seperator = when (compileTarget) {
            is Kson -> "\n"
            is Json, is Toml -> ",\n"
            is Yaml -> throw UnsupportedOperationException("We never format YAML objects as delimited")
        }

        return """
            |${indent.firstLineIndent()}{
            |${
            properties.withIndex().joinToString(seperator) { (index, property) ->
                val nodeAfterThisChild = properties.getOrNull(index + 1) ?: nextNode
                property.toSourceWithNext(indent.next(false), nodeAfterThisChild, compileTarget)
            }
        }
            |${indent.bodyLinesIndent()}}
            """.trimMargin()

    }

    private fun formatCompactObject(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        val outputObject = properties.withIndex().joinToString("") { (index, property) ->
            val nodeAfterThisChild = properties.getOrNull(index + 1) ?: nextNode
            val result = property.toSourceWithNext(indent, nodeAfterThisChild, compileTarget)

            // Only add space after this property if not using a space could result in ambiguity with the next node
            val needsSpace = index < properties.size - 1 &&
                    property is ObjectPropertyNodeImpl &&
                    result.last() != '\n' &&
                    when (property.value) {
                        is QuotedStringNode -> {
                            StringUnquoted.isUnquotable(property.value.stringContent)
                        }

                        is UnquotedStringNode,
                        is NumberNode,
                        is TrueNode,
                        is FalseNode,
                        is NullNode -> true

                        else -> false
                    }

            if (needsSpace) "$result " else result
        }
        return if (nextNode is ObjectPropertyNode) {
            // If the last property is a number we need to add whitespace before the '.' to prevent it becoming a number
            val needsSpace = (properties.last() as ObjectPropertyNodeImpl).value is NumberNode
            outputObject + if (needsSpace) " ." else "."
        } else outputObject
    }

    private fun formatUndelimitedObject(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        val outputObject = properties.withIndex().joinToString("\n") { (index, property) ->
            val nodeAfterThisChild = properties.getOrNull(index + 1) ?: nextNode
            if (index == 0) {
                property.toSourceWithNext(indent, nodeAfterThisChild, compileTarget)
            } else {
                // ensure subsequent properties do not think they are hanging
                property.toSourceWithNext(indent.clone(false), nodeAfterThisChild, compileTarget)
            }
        }

        /**
         * Only need to explicitly end this object with a [org.kson.parser.TokenType.DOT] if the next
         * thing in this document is an [ObjectPropertyNode] that does not belong to this object
         */
        return if (compileTarget is Kson && nextNode is ObjectPropertyNode) {
            "$outputObject\n${indent.bodyLinesIndent()}."
        } else {
            // put a newline after multi-property objects
            outputObject + if (properties.size > 1) "\n" else ""
        }
    }

    private fun formatTomlObject(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        // If we're in a hanging indent context (inside an inline array) or if we're indented (inside a multi-line array),
        // use inline table syntax
        val isInArray = indent.hangingIndent || indent.bodyLinesIndent().isNotEmpty()
        if (isInArray) {
            return formatTomlInlineTable(indent, compileTarget)
        }
        
        // TOML: Convert nested structure to flattened dotted keys, then group by top-level table
        val flattenedProperties = flattenToTomlDottedKeys("", compileTarget)

        val lines = mutableListOf<String>()

        // We'll iterate through flattened properties in order and group dotted keys by their first segment
        var i = 0
        while (i < flattenedProperties.size) {
            val (comments, key, value) = flattenedProperties[i]

            val dotIndex = key.indexOf('.')
            if (dotIndex == -1) {
                // Top-level key, emit directly
                val commentLines = if (compileTarget.preserveComments && comments.isNotEmpty()) {
                    comments.joinToString("\n") + "\n"
                } else ""
                lines.add(commentLines + "$key = $value")
                i++
            } else {
                // Group all keys that start with this top-level segment
                val top = key.substring(0, dotIndex)
                // Emit table header
                lines.add("[$top]")

                // Collect all entries that belong to this table (prefix match)
                while (i < flattenedProperties.size) {
                    val (c2, k2, v2) = flattenedProperties[i]
                    if (!k2.startsWith(top + ".")) break
                    val subKey = k2.substring(top.length + 1)
                    val commentLines = if (compileTarget.preserveComments && c2.isNotEmpty()) {
                        c2.joinToString("\n") + "\n"
                    } else ""
                    lines.add(commentLines + "$subKey = $v2")
                    i++
                }
                // Add a separating blank line between this table and subsequent top-level entries
                if (i < flattenedProperties.size) {
                    lines.add("")
                }
            }
        }

        return lines.joinToString("\n")
    }
    
    /**
     * Format object as TOML inline table: {key1 = val1, key2 = val2}
     * Used when object appears inside an array
     */
    internal fun formatTomlInlineTable(indent: Indent, compileTarget: CompileTarget): String {
        if (properties.isEmpty()) {
            return indent.firstLineIndent() + "{}"
        }
        
        val emptyIndent = Indent(IndentType.Space(2), 0, false)
        val inlineProps = properties.filterIsInstance<ObjectPropertyNodeImpl>().joinToString(", ") { property ->
            val key = property.key.toSourceWithNext(emptyIndent, property.value, compileTarget)
            val value = when (val v = property.value) {
                is ObjectNode -> v.formatTomlInlineTable(emptyIndent.copy(hangingIndent = true), compileTarget)
                is ListNode -> v.toSourceWithNext(emptyIndent.copy(hangingIndent = true), null, compileTarget).trim()
                is EmbedBlockNode -> {
                    // Expand embed blocks with dotted notation in inline tables
                    val embedProps = v.toTomlInlineProperties(key, compileTarget as Toml)
                    return@joinToString embedProps.joinToString(", ")
                }
                else -> v.toSourceWithNext(emptyIndent, null, compileTarget).trim()
            }
            "$key = $value"
        }
        
        return indent.firstLineIndent() + "{$inlineProps}"
    }
    
    /**
     * Recursively flatten nested objects into TOML dotted key notation
     * Returns a list of (comments, dottedKey, value) triples
     */
    private fun flattenToTomlDottedKeys(
        prefix: String,
        compileTarget: CompileTarget
    ): List<Triple<List<String>, String, String>> {
        val result = mutableListOf<Triple<List<String>, String, String>>()
        
        properties.filterIsInstance<ObjectPropertyNodeImpl>().forEach { property ->
            val key = property.key.toSourceWithNext(Indent(), property.value, compileTarget)
            val fullKey = if (prefix.isEmpty()) key else "$prefix.$key"
            
            when (val value = property.value) {
                is ObjectNode -> {
                        // Special-case: when an object represents embedContent-like nested keys
                        if (key == EmbedObjectKeys.EMBED_CONTENT.key) {
                            // For embedContent objects that are not plain string embed blocks, emit their
                            // child properties as dotted keys whose values are TOML triple-quoted strings
                            value.properties.filterIsInstance<ObjectPropertyNodeImpl>().forEachIndexed { idx, subProp ->
                                val subKey = subProp.key.toSourceWithNext(Indent(), subProp.value, compileTarget)
                                var subValue = subProp.value.toSourceWithNext(Indent(), null, compileTarget).trim()
                                // If the value is quoted, unescape it to get the raw content
                                if (subValue.startsWith("\"") && subValue.endsWith("\"")) {
                                    subValue = unescapeStringContent(subValue.substring(1, subValue.length - 1))
                                }
                                val triple = "\"\"\"\n${subValue}\n\"\"\""
                                val commentsForThis = if (idx == 0) property.comments else emptyList()
                                result.add(Triple(commentsForThis, "$fullKey.$subKey", triple))
                            }
                        } else {
                            // Recursively flatten nested objects
                            result.addAll(value.flattenToTomlDottedKeys(fullKey, compileTarget))
                        }
                }
                is EmbedBlockNode -> {
                    // For TOML, always flatten embed blocks into dotted keys
                    val tomlOutput = value.toSourceWithNext(Indent(), null, compileTarget).trim()
                    
                    // Check if output starts with [embedBlock] table header
                    val lines = tomlOutput.lines()
                    var idx = 0
                    var tagRaw: String? = null
                    var metadataRaw: String? = null
                    var contentRaw: String? = null
                    
                    // Skip [embedBlock] header if present
                    if (lines.firstOrNull()?.trim() == "[embedBlock]") {
                        idx = 1
                    }

                    // Parse the embed block TOML output into discrete parts
                    while (idx < lines.size) {
                        val line = lines[idx]
                        val trimmedLine = line.trim()
                        if (trimmedLine.isNotEmpty() && !trimmedLine.startsWith("#")) {
                            val parts = trimmedLine.split(" = ", limit = 2)
                            if (parts.size == 2) {
                                val subKey = parts[0].trim()
                                var rhs = parts[1]
                                if (rhs.trimStart().startsWith("\"\"\"")) {
                                    // consume until closing triple-quote
                                    val buffer = StringBuilder(rhs)
                                    var j = idx + 1
                                    while (j < lines.size) {
                                        val nl = lines[j]
                                        buffer.append("\n").append(nl)
                                        if (nl.trim().endsWith("\"\"\"")) {
                                            idx = j
                                            break
                                        }
                                        j++
                                    }
                                    rhs = buffer.toString()
                                }

                                when (subKey) {
                                    EmbedObjectKeys.EMBED_TAG.key -> tagRaw = rhs
                                    EmbedObjectKeys.EMBED_METADATA.key -> metadataRaw = rhs
                                    EmbedObjectKeys.EMBED_CONTENT.key -> contentRaw = rhs
                                }
                            }
                        }
                        idx++
                    }

                    // Emit entries with fullKey prefix (ignore metadata in TOML)
                    if (tagRaw != null) {
                        result.add(Triple(property.comments, "$fullKey.${EmbedObjectKeys.EMBED_TAG.key}", tagRaw))
                    }
                    if (contentRaw != null) {
                        result.add(Triple(emptyList(), "$fullKey.${EmbedObjectKeys.EMBED_CONTENT.key}", contentRaw))
                    }
                }
                else -> {
                        // Special-case: if this property is named embedContent, prefer emitting TOML
                        // triple-quoted multiline basic strings so embedded content is preserved
                        if (key == EmbedObjectKeys.EMBED_CONTENT.key && value is StringNodeImpl) {
                            var content = value.processedStringContent
                            // remove a single trailing newline (we'll add canonical newlines in TOML)
                            if (content.endsWith("\n")) content = content.removeSuffix("\n")
                            val triple = "\"\"\"\n${content}\n\"\"\""
                            result.add(Triple(property.comments, fullKey, triple))
                        } else {
                            // Use hangingIndent = true to signal that we're in a property context
                            // This prevents arrays from using the "value = ..." format
                            val propertyIndent = Indent(IndentType.Space(2), 0, hangingIndent = true)
                            val valueStr = value.toSourceWithNext(propertyIndent, null, compileTarget).trim()
                            result.add(Triple(property.comments, fullKey, valueStr))
                        }
                }
            }
        }
        
        return result
    }
}

interface ObjectKeyNode : StringNode
class ObjectKeyNodeError(content: String, location: Location) : ObjectKeyNode, AstNodeError(content, location)
class ObjectKeyNodeImpl(
    val key: StringNode
) : ObjectKeyNode, AstNodeImpl(key.location) {
    override fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        val keyOutput = when (compileTarget) {
            is Toml -> {
                // For TOML, output unquoted keys when possible
                val keyString = (key as? StringNodeImpl)?.stringContent ?: key.toSourceWithNext(indent, null, compileTarget)
                // TOML reserved keywords that must be quoted
                val tomlReservedKeywords = setOf("true", "false", "null", "inf", "nan")
                // Check if the key can be unquoted (simple identifier and not a reserved word)
                if (keyString.matches(Regex("^[A-Za-z_][A-Za-z0-9_-]*$")) && 
                    !tomlReservedKeywords.contains(keyString.lowercase())) {
                    keyString
                } else {
                    "\"${keyString.replace("\"", "\\\"")}\""
                }
            }
            else -> key.toSourceWithNext(indent, null, compileTarget)
        }
        return when (compileTarget) {
            is Toml -> keyOutput
            else -> "$keyOutput:"
        }
    }
}

interface ObjectPropertyNode : AstNode
class ObjectPropertyNodeError(content: String, location: Location) : ObjectPropertyNode, AstNodeError(content, location)
class ObjectPropertyNodeImpl(
    val key: ObjectKeyNode,
    val value: KsonValueNode,
    override val comments: List<String>,
    location: Location
) :
    ObjectPropertyNode, AstNodeImpl(location), Documented {
    override fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        return when (compileTarget) {
            is Kson -> {
                when (compileTarget.formatConfig.formattingStyle) {
                    FormattingStyle.DELIMITED -> delimitedObjectProperty(indent, nextNode, compileTarget)
                    FormattingStyle.COMPACT -> compactObjectProperty(indent, nextNode, compileTarget)
                    FormattingStyle.PLAIN -> undelimitedObjectProperty(indent, nextNode, compileTarget)
                }
            }

            is Yaml, is Toml -> undelimitedObjectProperty(indent, nextNode, compileTarget)
            is Json -> delimitedObjectProperty(indent, nextNode, compileTarget)
        }
    }

    private fun delimitedObjectProperty(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        val delimitedPropertyIndent = if (value is ListNode || value is ObjectNode ||
            // check if we're compiling an embed block to an object
            (compileTarget is Json && value is EmbedBlockNode && compileTarget.retainEmbedTags)
        ) {
            // For delimited lists and objects, don't increase their indent here - they provide their own indent nest
            indent.clone(true)
        } else {
            // otherwise, increase the indent
            indent.next(true)
        }
    val sep = if (compileTarget is Toml) " = " else " "
    return key.toSourceWithNext(indent, value, compileTarget) + sep +
        value.toSourceWithNext(delimitedPropertyIndent, nextNode, compileTarget)
    }

    private fun undelimitedObjectProperty(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        // Check if the value should be rendered on a new line (for complex types)
        val isEmbedBlockWithTags = value is EmbedBlockNode && when (compileTarget) {
            is Yaml -> compileTarget.retainEmbedTags
            is Toml -> (compileTarget as Toml).retainEmbedTags
            else -> false
        }
        
        val shouldUseNewLine = (value is ListNode && value.elements.isNotEmpty()) ||
                               (value is ObjectNode && value.properties.isNotEmpty()) ||
                               isEmbedBlockWithTags
        
        return if (shouldUseNewLine) {
            // For non-empty lists and objects, put the value on the next line
            // TOML doesn't use ':' - it will use dotted keys handled elsewhere
            val keySuffix = if (compileTarget is Toml) "" else ""
            key.toSourceWithNext(indent, value, compileTarget) + keySuffix + "\n" +
                    value.toSourceWithNext(indent.next(false), nextNode, compileTarget)
        } else {
            val sep = if (compileTarget is Toml) " = " else " "
            key.toSourceWithNext(indent, value, compileTarget) + sep +
                    value.toSourceWithNext(indent.next(true), nextNode, compileTarget)
        }
    }

    private fun compactObjectProperty(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        // A comment always needs to start on a new line. This can happen either when the first property or the next
        // node is commented.
        val firstPropertyHasComments = if (value !is ObjectNode) {
            false
        } else {
            when (val firstProperty = value.properties.firstOrNull()) {
                is ObjectPropertyNodeImpl -> firstProperty.comments.isNotEmpty()
                null -> false
                else -> false
            }
        }
        val nextNodeHasComments = (nextNode is Documented && nextNode.comments.isNotEmpty())

        val keyPrefix = key.toSourceWithNext(indent, value, compileTarget) +
                if (compileTarget is Toml) " = " else ""

        return keyPrefix +
                if (firstPropertyHasComments) {
                    "\n"
                } else {
                    ""
                } +
                value.toSourceWithNext(indent, nextNode, compileTarget) +
                if (nextNodeHasComments) "\n" else ""
    }
}

class ListNode(
    val elements: List<ListElementNode>,
    location: Location
) : KsonValueNodeImpl(location) {
    private sealed class ListDelimiters(val open: Char, val close: Char) {
        data object AngleBrackets : ListDelimiters('<', '>')
        data object SquareBrackets : ListDelimiters('[', ']')
    }

    override fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        val listDelimiter = when (compileTarget) {
            is Kson -> ListDelimiters.AngleBrackets
            is Yaml, is Json, is Toml -> ListDelimiters.SquareBrackets
            else -> ListDelimiters.SquareBrackets
        }
        if (elements.isEmpty()) {
            return "${indent.firstLineIndent()}${listDelimiter.open}${listDelimiter.close}"
        }
        return when (compileTarget) {
            is Kson -> {
                when (compileTarget.formatConfig.formattingStyle) {
                    FormattingStyle.PLAIN -> {
                        val outputList = formatUndelimitedList(indent, nextNode, compileTarget)
                        /**
                         * Only need to explicitly end this list with a [org.kson.parser.TokenType.DOT] if the next
                         * thing in this document is a [ListElementNode] that does not belong to this list
                         */
                        if (nextNode is ListElementNode) {
                            "$outputList\n${indent.bodyLinesIndent()}="
                        } else {
                            outputList
                        }
                    }

                    FormattingStyle.DELIMITED -> formatDelimitedList(indent, nextNode, compileTarget, listDelimiter)
                    FormattingStyle.COMPACT -> formatCompactList(
                        indent,
                        nextNode,
                        compileTarget,
                        ListDelimiters.SquareBrackets
                    )
                }
            }

            is Yaml -> formatUndelimitedList(indent, nextNode, compileTarget)
            is Json -> formatDelimitedList(indent, nextNode, compileTarget, listDelimiter)
            is Toml -> formatDelimitedList(indent, nextNode, compileTarget, listDelimiter)
        }
    }

    private fun formatDelimitedList(
        indent: Indent,
        nextNode: AstNode?,
        compileTarget: CompileTarget,
        listDelimiters: ListDelimiters
    ): String {
        // TOML arrays: use multi-line for arrays with many elements, inline for short arrays
        if (compileTarget is Toml) {
            // Check if we have elements with comments at root level
            val isRootLevel = indent.bodyLinesIndent().isEmpty()
            val isDocumentRoot = isRootLevel && !indent.hangingIndent
            val elementsWithComments = elements.filterIsInstance<ListElementNodeImpl>()
                .filter { it.comments.isNotEmpty() }
            
            // When at document root (not inside an object property) with comments, use preserveComments = false
            // and format as separate "value = ..." lines (TOML special format for root arrays)
            // For arrays inside objects or nested arrays, ALWAYS use preserveComments = true to put comments inside brackets
            val actualCompileTarget = if (isDocumentRoot && elementsWithComments.isNotEmpty()) {
                Toml(preserveComments = false)
            } else if (!isDocumentRoot && elementsWithComments.isNotEmpty() && compileTarget.preserveComments) {
                Toml(preserveComments = true)
            } else {
                Toml(preserveComments = false)
            }
            
            // Special case: Document root array with multiple commented elements
            // Format as separate "value = ..." lines (TOML convention for root-level arrays with comments)
            if (isDocumentRoot && elementsWithComments.size > 1 && compileTarget.preserveComments) {
                return elements.withIndex().joinToString("\n") { (index, element) ->
                    val valueKey = "value"
                    val elementImpl = element as? ListElementNodeImpl
                    val comments = elementImpl?.comments?.map { it.trimEnd() } ?: emptyList()
                    val commentLines = if (comments.isNotEmpty()) comments.joinToString("\n") + "\n" else ""
                    
                    // Get element value without comments (using preserveComments = false)
                    val elementWithoutComments = Toml(preserveComments = false)
                    val elementValue = elementImpl?.value ?: (element as ListElementNodeImpl).value
                    
                    // If element is an ObjectNode, format it as inline table
                    val elementStr = if (elementValue is ObjectNode) {
                        // Force inline table format by using hanging indent context
                        elementValue.formatTomlInlineTable(Indent(IndentType.Space(2), 0, true), elementWithoutComments)
                    } else {
                        elementValue.toSourceWithNext(indent, null, elementWithoutComments).trim()
                    }
                    
                    "${commentLines}$valueKey = $elementStr"
                }
            }
            
            // Determine whether to use multi-line format:
            // - Always use multi-line if elements have comments and NOT at document root
            // - If parent is inline array at root level: always use inline  
            // - If this is a root-level array: use multi-line if 4+ elements
            // - If nested in multi-line parent: always use multi-line to match parent
            val shouldUseMultiLine = when {
                !isDocumentRoot && elementsWithComments.isNotEmpty() && compileTarget.preserveComments -> true // Multi-line for commented elements in objects
                indent.hangingIndent && isRootLevel -> false // Parent is inline array at root, force inline
                isRootLevel -> elements.size >= 4 // Root array: heuristic for readability
                else -> true // Nested or in multi-line parent: always multi-line
            }
            
            if (shouldUseMultiLine) {
                // Multi-line format for TOML
                // Don't use hanging indent for multi-line arrays - elements need proper indentation
                val nextIndent = indent.next(false)
                return indent.firstLineIndent() + listDelimiters.open + "\n" +
                    elements.withIndex().joinToString(",\n") { (index, element) ->
                        val nodeAfterThisChild = elements.getOrNull(index + 1) ?: nextNode
                        // Comments are handled automatically by toSourceWithNext when preserveComments is true
                        element.toSourceWithNext(nextIndent, nodeAfterThisChild, actualCompileTarget)
                    } + "\n" +
                    indent.bodyLinesIndent() + listDelimiters.close
            } else {
                // Inline format for simple TOML arrays
                // Use hanging indent to signal that we're inside an array
                val hangingIndent = Indent(IndentType.Space(2), 0, true)
                val inlineElements = elements.joinToString(", ") { element ->
                    element.toSourceWithNext(hangingIndent, null, actualCompileTarget).trim()
                }
                return indent.firstLineIndent() + listDelimiters.open + inlineElements + listDelimiters.close
            }
        }
        
        val seperator = when (compileTarget) {
            is Kson -> "\n"
            is Json -> ",\n"
            else -> throw UnsupportedOperationException("We never format YAML objects as delimited")
        }

        // We pad our list bracket with newlines if our list is non-empty
        val bracketPadding = "\n"
        
        return indent.firstLineIndent() + listDelimiters.open + bracketPadding +
                elements.withIndex().joinToString(seperator) { (index, element) ->
                    val nodeAfterThisChild = elements.getOrNull(index + 1) ?: nextNode
                    element.toSourceWithNext(indent.next(false), nodeAfterThisChild, compileTarget)
                } +
                bracketPadding +
                indent.bodyLinesIndent() + listDelimiters.close
    }

    private fun formatCompactList(
        indent: Indent,
        nextNode: AstNode?,
        compileTarget: CompileTarget,
        listDelimiters: ListDelimiters
    ): String {
        return elements.withIndex().joinToString(
            "",
            prefix = listDelimiters.open.toString(),
            postfix = listDelimiters.close.toString()
        ) { (index, element) ->
            val nodeAfterThisChild = elements.getOrNull(index + 1) ?: nextNode
            val elementString = if (element is Documented && element.comments.isNotEmpty()) {
                "\n"
            } else {
                ""
            } + element.toSourceWithNext(indent.clone(hanging = true), nodeAfterThisChild, compileTarget)


            val isNotLastElement = index < elements.size - 1

            // Extract current and next element values for type checking
            val currentValue = (element as? ListElementNodeImpl)?.value
            val currentIsObject = currentValue is ObjectNode
            val nextIsObject = (nodeAfterThisChild as? ListElementNodeImpl)?.value is ObjectNode

            // Determine formatting based on context
            when {
                // Both objects need a dot separator, with space if current ends with number
                (isNotLastElement && currentIsObject && nextIsObject) -> "{$elementString}"

                // Add space between elements in a list, except when the element is a list
                isNotLastElement && element is ListElementNodeImpl && element.value !is ListNode -> {
                    "$elementString "
                }

                else -> elementString
            }
        }
    }

    private fun formatUndelimitedList(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        return elements.withIndex().joinToString("\n") { (index, element) ->
            val nodeAfterThisChild = elements.getOrNull(index + 1) ?: nextNode
            element.toSourceWithNext(indent, nodeAfterThisChild, compileTarget)
        }
    }
}

interface ListElementNode : AstNode
class ListElementNodeError(content: String, location: Location) : AstNodeError(content, location), ListElementNode
class ListElementNodeImpl(val value: KsonValueNode, override val comments: List<String>, location: Location) :
    ListElementNode, AstNodeImpl(location), Documented {

    override fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        return when (compileTarget) {
            is Kson -> {
                when (compileTarget.formatConfig.formattingStyle) {
                    FormattingStyle.PLAIN -> formatWithDash(indent, nextNode, compileTarget)
                    FormattingStyle.DELIMITED -> formatWithDash(indent, nextNode, compileTarget, isDelimited = true)
                    FormattingStyle.COMPACT -> value.toSourceWithNext(indent, nextNode, compileTarget)
                }
            }

            is Yaml -> formatWithDash(indent, nextNode, compileTarget)

            is Json, is Toml -> value.toSourceWithNext(indent, nextNode, compileTarget)
        }
    }

    private fun formatWithDash(
        indent: Indent,
        nextNode: AstNode?,
        compileTarget: CompileTarget,
        isDelimited: Boolean = false
    ): String {
        return if ((value is ListNode && value.elements.isNotEmpty()) && !isDelimited) {
            indent.bodyLinesIndent() + "- \n" + value.toSourceWithNext(
                indent.next(false),
                nextNode,
                compileTarget
            )
        } else {
            indent.bodyLinesIndent() + "- " + value.toSourceWithNext(
                indent.next(true),
                nextNode,
                compileTarget
            )
        }
    }
}

interface StringNode : KsonValueNode
abstract class StringNodeImpl(location: Location) : StringNode, KsonValueNodeImpl(location) {
    abstract val stringContent: String

    val processedStringContent: String by lazy {
        unescapeStringContent(stringContent)
    }
}

/**
 * Note: [ksonEscapedStringContent] is expected to be the exact content of a [stringQuote]-delimited [Kson] string,
 *   including all escapes, but excluding the outer quotes.  A [Kson] string is escaped identically to a Json string,
 *   except that [Kson] allows raw whitespace to be embedded in strings
 */
open class QuotedStringNode(
    private val ksonEscapedStringContent: String,
    private val stringQuote: StringQuote,
    location: Location
) : StringNodeImpl(location) {

    /**
     * An "unquoted" Kson string: i.e. a valid Kson string with all escapes intact except for quote escapes.
     * This string must be [SingleQuote]'ed or [DoubleQuote]'ed and then quote-escaped with [StringQuote.escapeQuotes]
     * to obtain a fully valid KsonString
     */
    private val unquotedString: String by lazy {
        stringQuote.unescapeQuotes(ksonEscapedStringContent)
    }

    override val stringContent: String by lazy {
        unquotedString
    }

    override fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        return when (compileTarget) {
            is Kson -> {
                // Check if we can use this string unquoted
                val isSimple = StringUnquoted.isUnquotable(unquotedString)

                indent.firstLineIndent() +
                        if (isSimple) {
                            unquotedString
                        } else {
                            val singleQuoteCount = SingleQuote.countDelimiterOccurrences(unquotedString)
                            val doubleQuoteCount = DoubleQuote.countDelimiterOccurrences(unquotedString)

                            // prefer single-quotes unless double-quotes would require less escaping
                            val chosenDelimiter = if (doubleQuoteCount < singleQuoteCount) {
                                DoubleQuote
                            } else {
                                SingleQuote
                            }

                            val escapedContent = chosenDelimiter.escapeQuotes(unquotedString)
                            "${chosenDelimiter}$escapedContent${chosenDelimiter}"
                        }
            }

            is Yaml -> {
                indent.firstLineIndent() + "\"" + DoubleQuote.escapeQuotes(unquotedString) + "\""
            }

            is Json -> {
                indent.firstLineIndent() + "\"${escapeRawWhitespace(DoubleQuote.escapeQuotes(unquotedString))}\""
            }
            
            is Toml -> {
                // For TOML, escape raw whitespace, then normalize
                val escaped = escapeRawWhitespace(unquotedString)
                
                // Heuristic to distinguish embedContent from regular strings:
                // - embedContent typically has \n only at the very end
                // - regular strings with mixed whitespace have \n in the middle, or have \t
                // So: convert newlines to spaces UNLESS the string has \n only at the end (no \n before)
                val hasInternalNewlines = escaped.contains("\\n") && !escaped.matches(Regex("""^[^\\]*(?:\\[^n\\][^\\]*)*(?:\\n)?$"""))
                val hasTabs = escaped.contains("\\t")
                
                val normalized = if (hasInternalNewlines || hasTabs) {
                    // String has tabs or internal newlines - convert all newlines to spaces
                    normalizeWhitespaceForToml(convertNewlineEscapesToSpaces(escaped))
                } else {
                    // String has at most one \n at the end (likely embedContent) - preserve \n
                    normalizeWhitespaceForToml(escaped)
                }
                
                indent.firstLineIndent() + "\"$normalized\""
            }
        }
    }
}

class UnquotedStringNode(override val stringContent: String, location: Location) : StringNodeImpl(location) {
    val yamlReservedKeywords = setOf(
        // Boolean true values
        "y", "Y", "yes", "Yes", "YES",
        "true", "True", "TRUE",
        "on", "On", "ON",
        // Boolean false values
        "n", "N", "no", "No", "NO",
        "false", "False", "FALSE",
        "off", "Off", "OFF",
        // Null values
        "null", "Null", "NULL"
    )

    override fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        return when (compileTarget) {
            is Kson -> {
                indent.firstLineIndent() + stringContent
            }

            is Yaml -> {
                indent.firstLineIndent() + if (yamlReservedKeywords.contains(stringContent)) {
                    "$DoubleQuote$stringContent$DoubleQuote"
                } else {
                    stringContent
                }
            }

            is Json -> {
                indent.firstLineIndent() + "\"${renderForJsonString(stringContent)}\""
            }
            
            is Toml -> {
                indent.firstLineIndent() + "\"${normalizeWhitespaceForToml(renderForJsonString(stringContent))}\""
            }
        }
    }
}

/**
 * Callers are in charge of ensuring that `stringValue` is parseable by [NumberParser]
 */
class NumberNode(stringValue: String, location: Location) : KsonValueNodeImpl(location) {
    val value: ParsedNumber by lazy {
        val parsedNumber = NumberParser(stringValue).parse()
        parsedNumber.number ?: throw IllegalStateException(
            "Hitting this indicates a parser bug: unparseable " +
                    "strings should be passed here but we got: " + stringValue
        )
    }

    override fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        return when (compileTarget) {
            is Kson, is Yaml, is Json, is Toml -> {
                indent.firstLineIndent() + value.asString
            }
        }
    }
}

class TrueNode(location: Location) : KsonValueNodeImpl(location) {
    override fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        return when (compileTarget) {
            is Kson, is Yaml, is Json, is Toml -> {
                indent.firstLineIndent() + "true"
            }
        }
    }
}

class FalseNode(location: Location) : KsonValueNodeImpl(location) {
    override fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        return when (compileTarget) {
            is Kson, is Yaml, is Json, is Toml -> {
                indent.firstLineIndent() + "false"
            }
        }
    }
}

class NullNode(location: Location) : KsonValueNodeImpl(location) {
    override fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        return when (compileTarget) {
            is Toml -> {
                // TOML does not have a native null literal; represent it as the string "null"
                indent.firstLineIndent() + "\"null\""
            }
            is Kson, is Yaml, is Json -> {
                indent.firstLineIndent() + "null"
            }
            else -> indent.firstLineIndent() + "null"
        }
    }
}

class EmbedBlockNode(
    val embedTagNode: StringNodeImpl?,
    val metadataTagNode: StringNodeImpl?,
    val embedContentNode: StringNodeImpl,
    embedDelim: EmbedDelim,
    location: Location,
    private val isDecodedFromObject: Boolean = false
) :
    KsonValueNodeImpl(location) {

    // Remember which delimiter was used when this embed block was parsed so TOML
    // emission can make decisions based on the original delimiter.
    private val originalDelim: EmbedDelim = embedDelim

    private val embedTag: String = embedTagNode?.stringContent ?: ""
    private val metadataTag: String = metadataTagNode?.stringContent ?: ""
    private val embedContent: String by lazy {
        embedDelim.unescapeEmbedContent(embedContentNode.stringContent)
    }

    override fun toSourceInternal(indent: Indent, nextNode: AstNode?, compileTarget: CompileTarget): String {
        return when (compileTarget) {
            is Kson -> renderKsonFormat(indent, compileTarget)
            is Yaml -> renderYamlFormat(indent, compileTarget)
            is Json -> renderJsonFormat(indent, compileTarget)
            is Toml -> renderTomlFormat(indent, compileTarget)
        }
    }

    /**
     * Renders this embed block as KSON format source code
     *
     * @param indent The indentation to apply
     * @param compileTarget The KSON compile target with formatting configuration
     * @return The rendered KSON source string
     */
    private fun renderKsonFormat(indent: Indent, compileTarget: Kson): String {
        val (delimiter, content) = selectOptimalDelimiter()
        val embedPreamble = embedTag + if (metadataTag.isNotEmpty()) ": $metadataTag" else ""

        return when (compileTarget.formatConfig.formattingStyle) {
            FormattingStyle.PLAIN, FormattingStyle.DELIMITED -> {
                val indentedContent = content.lines().joinToString("\n${indent.bodyLinesIndent()}") { it }
                "${indent.firstLineIndent()}${delimiter.openDelimiter}$embedPreamble\n${indent.bodyLinesIndent()}$indentedContent${delimiter.closeDelimiter}"
            }
            FormattingStyle.COMPACT -> {
                val compactContent = content.lines().joinToString("\n") { it }
                "${delimiter.openDelimiter}$embedPreamble\n$compactContent${delimiter.closeDelimiter}"
            }
        }
    }

    /**
     * Selects the optimal delimiter for the embed block content, preferring delimiters that don't appear in the content
     * to avoid escaping. Returns a pair of the chosen delimiter and the content (escaped if necessary).
     *
     * @return A pair of the chosen [EmbedDelim] and the content string (escaped if the delimiter appears in content)
     */
    private fun selectOptimalDelimiter(): Pair<EmbedDelim, String> {
        val percentCount = EmbedDelim.Percent.countDelimiterOccurrences(embedContent)
        val dollarCount = EmbedDelim.Dollar.countDelimiterOccurrences(embedContent)

        return when {
            percentCount == 0 -> EmbedDelim.Percent to embedContent
            dollarCount == 0 -> EmbedDelim.Dollar to embedContent
            else -> {
                val delimiter = if (dollarCount < percentCount) EmbedDelim.Dollar else EmbedDelim.Percent
                delimiter to delimiter.escapeEmbedContent(embedContent)
            }
        }
    }

    /**
     * Renders this embed block as YAML format source code
     *
     * @param indent The indentation to apply
     * @param compileTarget The YAML compile target with configuration
     * @return The rendered YAML source string
     */
    private fun renderYamlFormat(indent: Indent, compileTarget: Yaml): String {
        return if (!compileTarget.retainEmbedTags) {
            renderMultilineYamlString(embedContent, indent, indent.next(false))
        } else {
            renderYamlObject(indent)
        }
    }

    /**
     * Renders this embed block as JSON format source code
     *
     * @param indent The indentation to apply
     * @param compileTarget The JSON compile target with configuration
     * @return The rendered JSON source string
     */
    private fun renderJsonFormat(indent: Indent, compileTarget: Json): String {
        return if (!compileTarget.retainEmbedTags) {
            indent.firstLineIndent() + "\"${renderForJsonString(embedContent)}\""
        } else {
            renderJsonObject(indent, indent.next(false))
        }
    }

    private fun renderTomlFormat(indent: Indent, compileTarget: Toml): String {
        // For TOML embed content we emit a TOML multiline basic string for embedContent
        // (triple-quoted) so that original embed formatting and delimiter-escaping is preserved.
        // When retainEmbedTags is true we emit tag/metadata properties followed by embedContent;
        // when false we emit embedTag (if present) and embedContent as top-level properties.

        // Choose the content literal depending on whether this node was decoded from an
        // object or parsed as an embed block. For object-decoded embed blocks we prefer
        // the unescaped content (so we don't emit wrapper delimiter lines). For parsed
        // embed blocks we prefer the raw parser literal so single-backslash escapes are
        // preserved.
        val rawLiteral = if (isDecodedFromObject) {
            embedContent
        } else {
            try {
                embedContentNode.stringContent
            } catch (e: Exception) {
                embedContent
            } ?: embedContent
        }

        // Trim a single trailing newline for consistent emission; we'll add a newline when building the block
        var contentTrimmed = if (rawLiteral.endsWith('\n')) rawLiteral.removeSuffix("\n") else rawLiteral

        // Remove common leading indentation from non-empty lines so embedded blocks that were
        // indented in source appear normalized in TOML triple-quoted output
        val lines = contentTrimmed.split("\n")
        val minIndent = lines.filter { it.isNotEmpty() }
            .map { it.takeWhile { c -> c == ' ' || c == '\t' }.length }
            .minOrNull() ?: 0
        if (minIndent > 0) {
            contentTrimmed = lines.joinToString("\n") { line ->
                if (line.length >= minIndent) line.substring(minIndent) else line
            }
        }

        // Determine whether we need to wrap the content with explicit embed delimiters
        // inside the TOML triple-quoted string. Historically, tests expect the original
        // delimiter marker lines to be preserved only when the original delimiter type
        // required it. So we only consider escape patterns that match the original
        // delimiter used when parsing this block.
        // Add an explicit wrapper when necessary for parsed embed blocks. We need to
        // preserve two separate cases:
        //  - parsed embed blocks that contain escaped-close sequences (e.g. "%\%")
        //    should have wrapper lines re-inserted so the original embed semantics
        //    are preserved
        //  - parsed embed blocks that somehow contain raw close delimiter substrings
        //    (e.g. "%%" or "$$") should also cause wrapping
        // For embed blocks decoded from objects we do NOT add wrapper lines.
        val needsWrapper = if (isDecodedFromObject) {
            false
        } else {
            when (originalDelim) {
                EmbedDelim.Percent -> contentTrimmed.contains("%\\%") || contentTrimmed.contains(originalDelim.closeDelimiter)
                EmbedDelim.Dollar -> contentTrimmed.contains("\\$\\$") || contentTrimmed.contains(originalDelim.closeDelimiter)
            }
        }

        val contentToEscape = if (needsWrapper) {
            // When wrapping for parsed embed blocks, always add explicit wrapper lines
            // (we disabled wrapping for object-decoded blocks earlier). Prefer the
            // percent delimiter as the canonical wrapper in output.
            val chosenDelim = EmbedDelim.Percent
            chosenDelim.openDelimiter + "\n" + contentTrimmed + "\n" + chosenDelim.closeDelimiter
        } else {
            contentTrimmed
        }

        // TOML multiline basic strings still interpret backslash escapes. For embed
        // blocks that were decoded from objects we need to preserve the additional
        // escaping they carried in the AST (so double backslashes in source), but
        // for parsed embed blocks we should preserve the parser literal as-is
        // (do not double backslashes). In all cases escape triple-quote sequences
        // to avoid terminating the TOML string early.
        val escaped = if (isDecodedFromObject) {
            contentToEscape.replace("\\", "\\\\").replace("\"\"\"", "\\\"\\\"\\\"")
        } else {
            contentToEscape.replace("\"\"\"", "\\\"\\\"\\\"")
        }

        val baseIndent = indent.firstLineIndent()

        if (!compileTarget.retainEmbedTags) {
            // Emit embedTag if present, then embedContent as a TOML triple-quoted basic string
            // When there's an embedTag, group fields under [embedBlock] table
            if (embedTag.isNotEmpty()) {
                val parts = mutableListOf<String>()
                parts.add("[embedBlock]")
                parts.add("${EmbedObjectKeys.EMBED_TAG.key} = \"${renderForJsonString(embedTag)}\"")
                parts.add("${EmbedObjectKeys.EMBED_CONTENT.key} = \"\"\"\n" + escaped + "\n\"\"\"")
                return parts.joinToString("\n")
            } else {
                // Without embedTag, emit embedContent directly at root level
                val contentKey = EmbedObjectKeys.EMBED_CONTENT.key
                val contentBlock = baseIndent + "$contentKey = \"\"\"\n" + escaped + "\n" + baseIndent + "\"\"\""
                return contentBlock
            }
        } else {
            // retainEmbedTags: always emit tag/metadata lines (if any) and embedContent as triple-quoted
            // When there's a tag or metadata, group fields under [embedBlock] table
            val parts = mutableListOf<String>()
            if (embedTag.isNotEmpty() || metadataTag.isNotEmpty()) {
                parts.add("[embedBlock]")
            }
            // TOML convention: when only metadata exists (no embedTag), emit it as embedTag for simplicity
            if (embedTag.isNotEmpty()) {
                parts.add("${EmbedObjectKeys.EMBED_TAG.key} = \"${renderForJsonString(embedTag)}\"")
                // Only emit embedMetadata if we also have an embedTag
                if (metadataTag.isNotEmpty()) {
                    parts.add("${EmbedObjectKeys.EMBED_METADATA.key} = \"${renderForJsonString(metadataTag)}\"")
                }
            } else if (metadataTag.isNotEmpty()) {
                // No embedTag but has metadata: emit metadata as embedTag
                parts.add("${EmbedObjectKeys.EMBED_TAG.key} = \"${renderForJsonString(metadataTag)}\"")
            }
            // Do not prefix content lines with extra indentation here; keep normalized content as-is
            parts.add("${EmbedObjectKeys.EMBED_CONTENT.key} = \"\"\"\n" + escaped + "\n\"\"\"")
            return parts.joinToString("\n")
        }
    }

    /**
     * Helper method to render this embed block as inline TOML properties for use in inline tables.
     * Returns a list of "key = value" strings that can be joined with ", " in an inline table context.
     *
     * @param keyPrefix The dotted key prefix to use for the embed properties (e.g., "source")
     * @param compileTarget The TOML compile target
     * @return List of "key = value" strings for inline table
     */
    internal fun toTomlInlineProperties(keyPrefix: String, compileTarget: Toml): List<String> {
        val parts = mutableListOf<String>()
        
        if (compileTarget.retainEmbedTags) {
            if (embedTag.isNotEmpty()) {
                parts.add("$keyPrefix.${EmbedObjectKeys.EMBED_TAG.key} = \"${renderForJsonString(embedTag)}\"")
            }
            if (metadataTag.isNotEmpty()) {
                parts.add("$keyPrefix.${EmbedObjectKeys.EMBED_METADATA.key} = \"${renderForJsonString(metadataTag)}\"")
            }
        } else {
            if (embedTag.isNotEmpty()) {
                parts.add("$keyPrefix.${EmbedObjectKeys.EMBED_TAG.key} = \"${renderForJsonString(embedTag)}\"")
            }
        }
        
        // For embedContent, use triple-quoted string with escaped content
        val content = embedContent.replace("\"\"\"", "\\\"\\\"\\\"")
        parts.add("$keyPrefix.${EmbedObjectKeys.EMBED_CONTENT.key} = \"\"\"${content}\"\"\"")
        
        return parts
    }

    private fun escapeForToml(s: String): String {
        // minimal escaping for double quotes and backslashes
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
    }

    /**
     * Renders this embed block as a JSON object with separate fields for embed tag, metadata, and content
     *
     * @param indent The base indentation to apply to the object
     * @param nextIndent The indentation for nested object properties
     * @return The rendered JSON object string
     */
    private fun renderJsonObject(indent: Indent, nextIndent: Indent): String {
        val embedTagLine = if (embedTag.isNotEmpty()) {
            "${nextIndent.bodyLinesIndent()}\"${EmbedObjectKeys.EMBED_TAG.key}\": \"$embedTag\",\n"
        } else ""

        val metadataTagLine = if (metadataTag.isNotEmpty()) {
            "${nextIndent.bodyLinesIndent()}\"${EmbedObjectKeys.EMBED_METADATA.key}\": \"$metadataTag\",\n"
        } else ""

        return """
            |${indent.firstLineIndent()}{
            |$embedTagLine$metadataTagLine${nextIndent.bodyLinesIndent()}"${EmbedObjectKeys.EMBED_CONTENT.key}": "${renderForJsonString(embedContent)}"
            |${indent.bodyLinesIndent()}}
        """.trimMargin()
    }

    /**
     * Renders this embed block as a YAML object with separate fields for embed tag, metadata, and content
     *
     * @param indent The indentation to apply to the object
     * @return The rendered YAML object string
     */
    private fun renderYamlObject(indent: Indent): String {
        val embedTagLine = if (embedTag.isNotEmpty()) {
            "${indent.firstLineIndent()}${EmbedObjectKeys.EMBED_TAG.key}: \"$embedTag\"\n"
        } else ""

        val metadataTagLine = if (metadataTag.isNotEmpty()) {
            "${indent.firstLineIndent()}${EmbedObjectKeys.EMBED_METADATA.key}: \"$metadataTag\"\n"
        } else ""

        return embedTagLine + metadataTagLine +
                "${indent.firstLineIndent()}${EmbedObjectKeys.EMBED_CONTENT.key}: " +
                renderMultilineYamlString(embedContent, indent.clone(true), indent.next(true))
    }

    /**
     * Formats a string as a Yaml multiline string, preserving indentation
     *
     * @param content The string content to format
     * @param indent The base indentation level
     * @param contentIndent Additional indentation to apply to the content
     * @return a Yaml-formatted multiline string with any needed indentation markers
     */
    private fun renderMultilineYamlString(
        content: String,
        indent: Indent,
        contentIndent: Indent
    ): String {
        // Find minimum leading whitespace across non-empty lines
        val contentIndentSize = content.split("\n")
            .filter { it.isNotEmpty() }
            .minOfOrNull { line -> line.takeWhile { it.isWhitespace() }.length } ?: 0

        // The user's content has an indent we must maintain, so we must tell Yaml how much indent
        // we are giving it on our multiline string to ensure it does not eat up the content's indent too
        val indentSize = contentIndent.bodyLinesIndent().length
        val multilineLineIndicator = if (contentIndentSize > 0) "|$indentSize" else "|"

        return indent.firstLineIndent() + multilineLineIndicator + "\n" +
                content.split("\n")
                    .joinToString("\n") { line ->
                        contentIndent.bodyLinesIndent() + line
                    }
    }
}
