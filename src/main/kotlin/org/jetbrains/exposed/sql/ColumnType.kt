package org.jetbrains.exposed.sql

import org.jetbrains.exposed.dao.*
import org.jetbrains.exposed.sql.statements.DefaultValueMarker
import org.jetbrains.exposed.sql.vendors.*
import java.io.InputStream
import java.math.*
import java.nio.ByteBuffer
import java.sql.*
import java.time.*
import java.time.format.DateTimeFormatter
import java.util.*
import javax.sql.rowset.serial.SerialBlob

interface IColumnType {
    var nullable: Boolean
    fun sqlType(): String

    fun valueFromDB(value: Any): Any  = value

    fun valueToString(value: Any?) : String {
        return when (value) {
            null -> {
                if (!nullable) error("NULL in non-nullable column")
                "NULL"
            }

            DefaultValueMarker -> "DEFAULT"

            is Iterable<*> -> {
                value.joinToString(","){ valueToString(it) }
            }

            else ->  {
                nonNullValueToString (value)
            }
        }
    }

    fun valueToDB(value: Any?): Any? = value?.let { notNullValueToDB(it) }

    fun notNullValueToDB(value: Any): Any  = value

    fun nonNullValueToString(value: Any) : String {
        return notNullValueToDB(value).toString()
    }

    fun readObject(rs: ResultSet, index: Int) = rs.getObject(index)

    fun setParameter(stmt: PreparedStatement, index: Int, value: Any?) {
        stmt.setObject(index, value)
    }
}

abstract class ColumnType(override var nullable: Boolean = false) : IColumnType {
    override fun toString(): String = sqlType()
}

class AutoIncColumnType(val delegate: ColumnType, private val _autoincSeq: String) : IColumnType by delegate {

    val autoincSeq : String? get() = if (currentDialect.needsSequenceToAutoInc) _autoincSeq else null

    private fun resolveAutIncType(columnType: IColumnType) : String = when (columnType) {
        is EntityIDColumnType<*> -> resolveAutIncType(columnType.idColumn.columnType)
        is IntegerColumnType -> currentDialect.dataTypeProvider.shortAutoincType()
        is LongColumnType -> currentDialect.dataTypeProvider.longAutoincType()
        else -> error("Unsupported type $delegate for auto-increment")
    }

    final override fun sqlType(): String = resolveAutIncType(delegate)
}

val IColumnType.isAutoInc: Boolean get() = this is AutoIncColumnType || (this is EntityIDColumnType<*> && idColumn.columnType.isAutoInc)
val Column<*>.autoIncSeqName : String? get() {
        return (columnType as? AutoIncColumnType)?.autoincSeq
            ?: (columnType as? EntityIDColumnType<*>)?.idColumn?.autoIncSeqName
}

class EntityIDColumnType<T:Any>(val idColumn: Column<T>) : ColumnType(false) {

    init {
        assert(idColumn.table is IdTable<*>){"EntityId supported only for IdTables"}
    }

    override fun sqlType(): String = idColumn.columnType.sqlType()

    override fun notNullValueToDB(value: Any): Any {
        return idColumn.columnType.notNullValueToDB(when (value) {
            is EntityID<*> -> value.value
            else -> value
        })
    }

    override fun valueFromDB(value: Any): Any {
        @Suppress("UNCHECKED_CAST")
        return when (value) {
            is EntityID<*> -> EntityID(value.value as T, idColumn.table as IdTable<T>)
            else -> EntityID(idColumn.columnType.valueFromDB(value) as T, idColumn.table as IdTable<T>)
        }
    }
}

class CharacterColumnType : ColumnType() {
    override fun sqlType(): String  = "CHAR"

    override fun valueFromDB(value: Any): Any {
        return when(value) {
            is Char -> value
            is Number -> value.toInt().toChar()
            is String -> value.single()
            else -> error("Unexpected value of type Char: $value")
        }
    }

    override fun notNullValueToDB(value: Any): Any {
        return valueFromDB(value).toString()
    }
}

class IntegerColumnType : ColumnType() {
    override fun sqlType(): String = currentDialect.dataTypeProvider.shortType()

    override fun valueFromDB(value: Any): Any {
        return when(value) {
            is Int -> value
            is Number -> value.toInt()
            else -> error("Unexpected value of type Int: $value")
        }
    }
}

class LongColumnType : ColumnType() {
    override fun sqlType(): String = currentDialect.dataTypeProvider.longType()

    override fun valueFromDB(value: Any): Any {
        return when(value) {
            is Long -> value
            is Number -> value.toLong()
            else -> error("Unexpected value of type Long: $value")
        }
    }
}

class DecimalColumnType(val precision: Int, val scale: Int): ColumnType() {
    override fun sqlType(): String  = "DECIMAL($precision, $scale)"
    override fun valueFromDB(value: Any): Any {
        val valueFromDB = super.valueFromDB(value)
        return when (valueFromDB) {
            is BigDecimal -> valueFromDB.setScale(scale, RoundingMode.HALF_EVEN)
            is Double -> BigDecimal.valueOf(valueFromDB).setScale(scale, RoundingMode.HALF_EVEN)
            is Int -> BigDecimal(valueFromDB)
            is Long -> BigDecimal.valueOf(valueFromDB)
            else -> valueFromDB
        }
    }
}

class EnumerationColumnType<T:Enum<T>>(val klass: Class<T>): ColumnType() {
    override fun sqlType(): String  = currentDialect.dataTypeProvider.shortType()

    override fun notNullValueToDB(value: Any): Any {
        return when (value) {
            is Int -> value
            is Enum<*> -> value.ordinal
            else -> error("$value is not valid for enum ${klass.name}")
        }
    }

    override fun valueFromDB(value: Any): Any {
        return when (value) {
            is Number -> klass.enumConstants!![value.toInt()]
            is Enum<*> -> value
            else -> error("$value is not valid for enum ${klass.name}")
        }
    }
}

class EnumerationNameColumnType<T:Enum<T>>(val klass: Class<T>, length: Int): StringColumnType(length) {
    override fun notNullValueToDB(value: Any): Any {
        return when (value) {
            is String -> value
            is Enum<*> -> value.name
            else -> error("$value is not valid for enum ${klass.name}")
        }
    }
    override fun valueFromDB(value: Any): Any {
        return when (value) {
            is String ->  klass.enumConstants!!.first { it.name == value }
            is Enum<*> -> value
            else -> error("$value is not valid for enum ${klass.name}")
        }
    }
}

private val DEFAULT_DATE_STRING_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd", Locale.ROOT)
private val DEFAULT_DATE_TIME_STRING_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS", Locale.ROOT)
private val DEFAULT_DATE_TIME_ZONE_STRING_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSSZ", Locale.ROOT)
private val SQLITE_DATE_STRING_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE
private val SQLITE_DATE_TIME_STRING_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")

class DateColumnType(): ColumnType() {
    override fun sqlType(): String  = "DATE"

    override fun nonNullValueToString(value: Any): String {
        if (value is String) return value
    
        val date = when (value) {
            is LocalDate      -> value
            is java.util.Date -> Instant.ofEpochMilli(value.time).atZone(ZoneId.systemDefault()).toLocalDate()
            else              -> error("Unexpected value: $value")
        }
        
        return "'${DEFAULT_DATE_STRING_FORMATTER.format(date)}'"
    }

    override fun valueFromDB(value: Any): Any = when(value) {
        is LocalDate      -> value
        is java.util.Date -> valueFromDB(value.time)
        is Int            -> valueFromDB(value.toLong())
        is Long           -> Instant.ofEpochMilli(value).atZone(ZoneId.systemDefault()).toLocalDate()
        is String         -> when (currentDialect) {
			SQLiteDialect -> LocalDate.parse(value, SQLITE_DATE_STRING_FORMATTER)
			else          -> value
		}
        // REVIEW
        else              -> LocalDate.parse(value.toString(), DEFAULT_DATE_STRING_FORMATTER)
    }

    override fun notNullValueToDB(value: Any): Any {
        if (value is LocalDate) {
            val millis = value.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli()
            return java.sql.Date(millis)
        }
        return value
    }
}

class DateTimeColumnType(val withTimezone : Boolean): ColumnType()
{
    override fun sqlType(): String = currentDialect.dataTypeProvider.dateTimeType(withTimezone)
    
    override fun nonNullValueToString(value : Any) : String
    {
        if (value is String)
            return value
        
        val dateTime = when (value) {
            is ZonedDateTime  -> value
            is java.util.Date -> Instant.ofEpochMilli(value.time).atZone(ZoneId.systemDefault())
            else              -> error("Unexpected value: $value")
        }
        
        return if (withTimezone)
            "'${DEFAULT_DATE_TIME_ZONE_STRING_FORMATTER.format(dateTime)}'"
        else
            "'${DEFAULT_DATE_TIME_STRING_FORMATTER.format(dateTime)}'"
    }
    
    override fun valueFromDB(value : Any) : Any = when (value) {
        is ZonedDateTime  -> value
        is java.util.Date -> valueFromDB(value.time)
        is Int            -> valueFromDB(value.toLong())
        is Long           -> Instant.ofEpochMilli(value).atZone(ZoneId.systemDefault())
        is String         -> when (currentDialect) {
            SQLiteDialect -> LocalDateTime.parse(value, SQLITE_DATE_TIME_STRING_FORMATTER).atZone(ZoneId.systemDefault())
            else          -> value
        }
        // REVIEW
        else              -> when (withTimezone) {
            true  -> ZonedDateTime.parse(value.toString(), DEFAULT_DATE_TIME_ZONE_STRING_FORMATTER)
            false -> LocalDateTime.parse(value.toString(), DEFAULT_DATE_TIME_STRING_FORMATTER).atZone(ZoneId.systemDefault())
        }
    }
    
    override fun notNullValueToDB(value : Any) : Any
    {
        if (value is ZonedDateTime) {
            val millis = value.toInstant().toEpochMilli()
            return java.sql.Timestamp(millis)
        }
        return value
    }
}

open class StringColumnType(val length: Int = 255, val collate: String? = null): ColumnType() {
    override fun sqlType(): String  {
        val ddl = StringBuilder()

        ddl.append(when (length) {
            in 1..255 -> "VARCHAR($length)"
            else -> currentDialect.dataTypeProvider.textType()
        })

        if (collate != null) {
            ddl.append(" COLLATE $collate")
        }

        return ddl.toString()
    }

    val charactersToEscape = mapOf(
            '\'' to "\'\'",
//            '\"' to "\"\"", // no need to escape double quote as we put string in single quotes
            '\r' to "\\r",
            '\n' to "\\n")

    override fun nonNullValueToString(value: Any): String {
        val beforeEscaping = value.toString()
        val sb = StringBuilder(beforeEscaping.length +2)
        sb.append('\'')
        for (c in beforeEscaping) {
            if (charactersToEscape.containsKey(c))
                sb.append(charactersToEscape[c])
            else
                sb.append(c)
        }
        sb.append('\'')
        return sb.toString()
    }

    override fun valueFromDB(value: Any): Any {
        if (value is java.sql.Clob) {
            return value.characterStream.readText()
        }
        return value
    }
}

class BinaryColumnType(val length: Int) : ColumnType() {
    override fun sqlType(): String  = currentDialect.dataTypeProvider.binaryType(length)

    // REVIEW
    override fun valueFromDB(value: Any): Any {
        if (value is java.sql.Blob) {
            return value.binaryStream.readBytes()
        }
        return value
    }
}

class BlobColumnType : ColumnType() {
    override fun sqlType(): String  = currentDialect.dataTypeProvider.blobType()

    override fun nonNullValueToString(value: Any): String {
        return "?"
    }

    override fun readObject(rs: ResultSet, index: Int): Any? {
        if (currentDialect.dataTypeProvider.blobAsStream)
            return SerialBlob(rs.getBytes(index))
        else
            return rs.getBlob(index)
    }

    override fun setParameter(stmt: PreparedStatement, index: Int, value: Any?) {
        if (currentDialect.dataTypeProvider.blobAsStream && value is InputStream) {
            stmt.setBinaryStream(index, value, value.available())
        } else {
            super.setParameter(stmt, index, value)
        }
    }

    override fun notNullValueToDB(value: Any): Any {
        if (currentDialect.dataTypeProvider.blobAsStream)
            return (value as Blob).binaryStream
        else
            return value
    }
}

class BooleanColumnType : ColumnType() {
    override fun sqlType(): String  = currentDialect.dataTypeProvider.booleanType()

    override fun valueFromDB(value: Any) = when (value) {
        is Number -> value.toLong() != 0L
        is String -> currentDialect.dataTypeProvider.booleanFromStringToBoolean(value)
        else -> value.toString().toBoolean()
    }

    override fun nonNullValueToString(value: Any) = currentDialect.dataTypeProvider.booleanToStatementString(value as Boolean)
}

class UUIDColumnType : ColumnType() {
    override fun sqlType(): String = currentDialect.dataTypeProvider.uuidType()

    override fun notNullValueToDB(value: Any): Any = currentDialect.dataTypeProvider.uuidToDB(when (value) {
        is UUID -> value
        is String -> UUID.fromString(value)
        is ByteArray -> ByteBuffer.wrap(value).let { UUID(it.long, it.long )}
        else -> error("Unexpected value of type UUID: ${value.javaClass.canonicalName}")
    })

    override fun valueFromDB(value: Any): Any = when(value) {
        is UUID -> value
        is ByteArray -> ByteBuffer.wrap(value).let { b -> UUID(b.long, b.long) }
        is String -> ByteBuffer.wrap(value.toByteArray()).let { b -> UUID(b.long, b.long) }
        else -> error("Unexpected value of type UUID: $value")
    }

}
