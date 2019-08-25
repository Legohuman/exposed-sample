package ru.dlevin.ex

import org.jetbrains.exposed.dao.Entity
import org.jetbrains.exposed.dao.EntityClass
import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.IdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.Function
import java.math.BigDecimal
import java.time.*
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.Date

open class StringUuidIdTable(name: String = "", columnName: String = "uuid") : IdTable<String>(name) {
    override val id: Column<EntityID<String>> = varchar(columnName, 255).primaryKey()
        .clientDefault { UUID.randomUUID().toString() }
        .entityId()
}

abstract class StringUuidEntity(id: EntityID<String>) : Entity<String>(id)

abstract class StringUuidEntityClass<out E : StringUuidEntity>(table: IdTable<String>, entityType: Class<E>? = null) :
    EntityClass<String, E>(table, entityType)


class InValuesListOrNotInValuesListOp<T>(
    val expr: ExpressionWithColumnType<T>,
    val list: Iterable<T>,
    val isInList: Boolean = true
) : Op<Boolean>() {

    override fun toSQL(queryBuilder: QueryBuilder): String = buildString {
        list.iterator().let { i ->
            if (!i.hasNext()) {
                val op = if (isInList) Op.FALSE else Op.TRUE
                append(op.toSQL(queryBuilder))
            } else {
                val first = i.next()
                if (!i.hasNext()) {
                    append(expr.toSQL(queryBuilder))
                    when {
                        isInList -> append(" = ")
                        else -> append(" != ")
                    }
                    append(queryBuilder.registerArgument(expr.columnType, first))
                } else {
                    append(expr.toSQL(queryBuilder))
                    when {
                        isInList -> append(" IN (VALUES ")
                        else -> append(" NOT IN (VALUES ")
                    }

                    queryBuilder.registerArguments(expr.columnType, list).joinTo(this, transform = { "($it)" })

                    append(")")
                }
            }
        }
    }
}


infix fun <T> ExpressionWithColumnType<T>.inValuesList(list: Iterable<T>): Op<Boolean> =
    InValuesListOrNotInValuesListOp(this, list, isInList = true)


class AvgOver<T : Comparable<T>, in S : T?>(val expr: Expression<in S>, scale: Int) :
    Function<BigDecimal?>(DecimalColumnType(Int.MAX_VALUE, scale)) {
    override fun toSQL(queryBuilder: QueryBuilder): String =
        "AVG(${expr.toSQL(queryBuilder)}) OVER (ORDER BY ${expr.toSQL(queryBuilder)} ASC)"
}

class CustomDateColumnType(val dateClass: Class<*>, val time: Boolean) : ColumnType() {
    private val DEFAULT_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss.SSSSSS")
    private val DEFAULT_DATE_FORMATTER = DateTimeFormatter.ofPattern("YYYY-MM-dd")

    override fun sqlType(): String = if (time) "TIMESTAMP" else "DATE"

    override fun nonNullValueToString(value: Any): String {
        if (value is String) return value

        val dateTime = when (value) {
            is LocalDateTime -> value
            is LocalDate -> value.atStartOfDay()
            is Date -> LocalDateTime.ofInstant(value.toInstant(), ZoneId.systemDefault())
            is java.sql.Date -> value.toLocalDate().atStartOfDay()
            is java.sql.Timestamp -> value.toLocalDateTime()
            else -> error("Unexpected value: $value of ${value::class.qualifiedName}")
        }

        return if (time)
            "'${DEFAULT_DATE_TIME_FORMATTER.format(dateTime)}'"
        else
            "'${DEFAULT_DATE_FORMATTER.format(dateTime)}'"
    }

    override fun valueFromDB(value: Any): Any =
        when (dateClass) {
            Date::class.java ->
                when (value) {
                    is LocalDateTime -> LocalDateTime.from(Instant.from(value))
                    is LocalDate -> LocalDate.from(Instant.from(value))
                    is Date -> value
                    is java.sql.Date -> Date(value.time)
                    is java.sql.Timestamp -> Date(value.time)
                    else -> when {
                        time -> DEFAULT_DATE_TIME_FORMATTER.parse(value.toString())
                        else -> DEFAULT_DATE_FORMATTER.parse(value.toString())
                    }
                }
            else-> TODO()
        }

    override fun notNullValueToDB(value: Any): Any {
        when (value) {
            is LocalDateTime -> {
                val instant = value.atZone(ZoneId.systemDefault()).toInstant()
                return if (time) {
                    java.sql.Timestamp.from(instant)
                } else {
                    java.sql.Date.from(instant)
                }
            }

            is LocalDate -> {
                val instant = value.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()
                return if (time) {
                    java.sql.Timestamp.from(instant)
                } else {
                    java.sql.Date.from(instant)
                }
            }

            is Date -> {
                return if (time) {
                    java.sql.Timestamp(value.time)
                } else {
                    java.sql.Date(value.time)
                }
            }

            is java.sql.Timestamp -> {
                return if (time) {
                    value
                } else {
                    java.sql.Date(value.time)
                }
            }

            is java.sql.Date -> {
                return if (time) {
                    java.sql.Timestamp(value.time)
                } else {
                    value
                }
            }
        }
        return value
    }
}