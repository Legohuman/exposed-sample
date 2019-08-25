package ru.dlevin.ex

import org.jetbrains.exposed.dao.EntityID
import org.jetbrains.exposed.dao.with
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.math.BigDecimal
import java.util.Date
import kotlin.system.measureTimeMillis

fun main(args: Array<String>) {
    Database.connect("jdbc:postgresql://localhost:5432/postgres", driver = "org.postgresql.Driver", user = "postgres")

    transaction {
        addLogger(StdOutSqlLogger)

//        SchemaUtils.create(ParagraphRevTable)

        batchInsertParagraphRevs()

        findByCustomClause()

        measureCustomClausePerformance()

        selectWithFunctions()

        selectWithWindowFunction()

        selectWithLimit()

        lazyCollectionSelect()

        lazyCollectionSelectMaterialized()

        collectionEagerLoad()

        referenceEagerLoad()

        measurePartialSelectPerformance()

        customColumnInsertAndSelect()


        selectWithJoin()

    }
}

private fun selectWithJoin() {
    val query = DocumentRevTable.innerJoin(DocumentTable)
        .slice(DocumentRevTable.columns)
        .select {
            DocumentTable.name notLike "N%"
        }.withDistinct()

    val revisions = DocumentRev.wrapRows(query).toList()
    println(revisions)
}

private fun customColumnInsertAndSelect() {
    val doc = Document.new {
        name = "Doc"
        description = "Desc"
        createdAt = Date()
        createdBy = "user"
    }

    val docs = Document.find { DocumentTable.name eq "Doc" }.toList()
    docs.forEach {
        println(it.createdAt)
    }
}

private fun measurePartialSelectPerformance() {
    repeat(5) {
        println(measureTimeMillis {
            ParagraphRev.wrapRows(
                ParagraphRevTable.slice(
                    ParagraphRevTable.id,
                    ParagraphRevTable.content
                ).selectAll().orderBy(ParagraphRevTable.order, SortOrder.DESC)
            ).toList()
                .forEach { it.content }
        })
    }

    repeat(5) {
        println(measureTimeMillis {
            ParagraphRev.all().orderBy(Pair(ParagraphRevTable.order, SortOrder.DESC)).toList()
                .forEach { it.content }
        })
    }
}

private fun referenceEagerLoad() {
    val revs = DocumentRev.all().with(DocumentRev::document).toList()
    revs.forEach { rev ->
        println("${rev.version} ${rev.isPublished}")
        println("${rev.document.name}")
    }
}

private fun collectionEagerLoad() {
    Document.find {
        DocumentTable.id inList (listOf(
            "cd2d4fec-4b20-49e5-afd9-7ef82142f1ac",
            "bcfceaa6-7d5f-42c2-9581-3dff51cab916"
        ))
    }.with(Document::revisions).forEach {
        it.revisions.toList().forEach { rev ->
            println("${rev.version} ${rev.isPublished}")
        }
    }
}

private fun lazyCollectionSelectMaterialized() {
    val document = DocumentRev.findById("bcfceaa6-7d5f-42c2-9581-3dff51cab915")?.document
    println(document?.name)
    val revisions = document?.revisions?.toList()
    revisions?.forEach {
        println("${it.version}, ${it.isPublished}")
    }

    revisions?.forEach {
        println("${it.version}, ${it.isPublished}")
    }
}

private fun lazyCollectionSelect() {
    val revisions = Document.findById("bcfceaa6-7d5f-42c2-9581-3dff51cab916")?.revisions?.toList()
    revisions?.forEach {
        println("${it.version}, ${it.isPublished}, ${it.document.name}")
    }

    revisions?.forEach {
        println("${it.version}, ${it.isPublished}, ${it.document.name}")
    }
}

private fun selectWithLimit() {
    DocumentTable.selectAll().limit(2, offset = 10).forEach {
        println(it)
    }
}

private fun selectWithWindowFunction() {
    val revId = ParagraphRevTable.id
    val revLevel = ParagraphRevTable.level
    val revRunningAvgLevel = AvgOver(revLevel, 10)
    ParagraphRevTable.slice(revId, revLevel, revRunningAvgLevel).selectAll().forEach {
        println("${it[revId]} - ${it[revLevel]} - ${it[revRunningAvgLevel]}")
    }
}

private fun selectWithFunctions() {
    val lcName = LowerCase(DocumentTable.name)
    val lcDesc = LowerCase(DocumentTable.description)
    val descOrName = Coalesce(DocumentTable.description, DocumentTable.name)
    DocumentTable.slice(lcName, lcDesc, descOrName).selectAll().forEach {
        println("${it[lcDesc]} - ${it[descOrName]} - ${it[lcName]}")
    }
}

private fun measureCustomClausePerformance() {
    repeat(10) {
        println(
            measureTimeMillis {
                Document.find { DocumentTable.name inValuesList ((1..4000).map { "N$it" }.toList()) }.forEach { }
            }
        )
    }
    repeat(10) {
        println(
            measureTimeMillis {
                Document.find { DocumentTable.name inList ((1..4000).map { "N$it" }.toList()) }.forEach { }
            }
        )
    }
}

private fun findByCustomClause() {
    Document.find { DocumentTable.name inValuesList ((1..4000).map { "N$it" }.toList()) or (DocumentTable.name eq "N13") }
        .forEach {
            println("${it.name}, ${it.createdAt}, ${it.createdBy}, ${it.description}")
        }
}

private fun batchInsertParagraphRevs() {
    val start = System.currentTimeMillis()
    val paragraphRevIds = ParagraphRevTable.batchInsert(0..10000) { i ->
        this[ParagraphRevTable.order] = BigDecimal(i)
        this[ParagraphRevTable.level] = i
        this[ParagraphRevTable.content] = "p$i"
        this[ParagraphRevTable.paragraph] = EntityID("a", ParagraphTable)
    }
    val finish = System.currentTimeMillis()
    println(finish - start)
    println("Inserted: ${paragraphRevIds.size}")
}



object DocumentTable : StringUuidIdTable("document") {
    val createdAt = registerColumn<Date>("created_at", CustomDateColumnType(Date::class.java, true))
    val createdBy = varchar("created_by", 255)
    val name = varchar("name", 255)
    val description = varchar("description", 255).nullable()
}

object ParagraphTable : StringUuidIdTable("paragraph") {
    val name = varchar("name", 255)
}

object DocumentRevTable : StringUuidIdTable("document_revision") {
    val document = reference("document_uuid", DocumentTable)
    val version = integer("version")
    val isPublished = bool("is_published")
}

object ParagraphRevTable : StringUuidIdTable("paragraph_revision") {
    val paragraph = reference("paragraph", ParagraphTable)
    val order = decimal("order", 1000, 500)
    val level = integer("level")
    val content = text("content")
}

object DocumentRevParagraphRevTable : Table("document_revision_paragraph_revision") {
    val documentRev = reference("document_revision_uuid", DocumentRevTable).primaryKey(0)
    val paragraphRev = reference("paragraph_revision_uuid", ParagraphRevTable).primaryKey(1)
}

class Document(id: EntityID<String>) : StringUuidEntity(id) {
    companion object : StringUuidEntityClass<Document>(DocumentTable)

    var createdBy: String by DocumentTable.createdBy
    var createdAt: Date by DocumentTable.createdAt
    var name: String by DocumentTable.name
    var description: String? by DocumentTable.description
    val revisions: SizedIterable<DocumentRev> by DocumentRev referrersOn DocumentRevTable.document
}

class DocumentRev(id: EntityID<String>) : StringUuidEntity(id) {
    companion object : StringUuidEntityClass<DocumentRev>(DocumentRevTable)

    var document by Document referencedOn DocumentRevTable.document
    var version: Int by DocumentRevTable.version
    var isPublished: Boolean by DocumentRevTable.isPublished
}

class ParagraphRev(id: EntityID<String>) : StringUuidEntity(id) {
    companion object : StringUuidEntityClass<ParagraphRev>(ParagraphRevTable)

    var order: BigDecimal by ParagraphRevTable.order
    var level: Int by ParagraphRevTable.level
    var content: String by ParagraphRevTable.content
}