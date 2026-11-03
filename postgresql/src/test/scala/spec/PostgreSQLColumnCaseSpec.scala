package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path
import java.sql.{Connection, DriverManager}

/**
 * `SQLStore` emits every identifier quoted (`SqlIdent.quote`), so a model field `legacyCamelField` becomes a
 * PostgreSQL column named `legacyCamelField`, case preserved, and a statement naming it must use that case: a quoted
 * `"legacycamelfield"` does not match it.
 *
 * Version A of a model has the camelCase field; version B removed it. Opening the table with version B keeps the
 * column (a store never drops a column on open) and reports it under its real case in `retiredColumns`; dropping it
 * explicitly with `dropRetiredColumns` removes it.
 */
@EmbeddedTest
class PostgreSQLColumnCaseSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with BeforeAndAfterAll with PostgreSQLAvailability {
  private val schema = "PostgreSQLColumnCaseSpec"
  private val table  = "schema_evolution_record"

  private def jdbcUrl: String = PostgreSQLTestSupport.jdbcUrl.get
  private def jdbcUser: String = PostgreSQLTestSupport.username
  private def jdbcPass: String = PostgreSQLTestSupport.password

  private def withJdbc[T](f: Connection => T): T = {
    val c = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)
    try f(c) finally c.close()
  }

  /** Drop the test schema so every run starts from a clean slate. */
  override def beforeAll(): Unit = {
    super.beforeAll()
    // `beforeAll` runs before per-test cancellation, so skip when PostgreSQL is unavailable.
    if PostgreSQLTestSupport.jdbcUrl.isDefined then withJdbc { c =>
      val st = c.createStatement()
      try st.executeUpdate(s"""DROP SCHEMA IF EXISTS "$schema" CASCADE""")
      finally st.close()
    }
  }

  /** Physical column names of the test table, exactly as Postgres
    * stores them (case preserved). */
  private def physicalColumns(): Set[String] = withJdbc { c =>
    val rs = c.getMetaData.getColumns(null, schema, table, null)
    val b = Set.newBuilder[String]
    while (rs.next()) b += rs.getString("COLUMN_NAME")
    b.result()
  }

  "PostgreSQLColumnCaseSpec" should {
    "initialize the table with a camelCase column" in {
      DBBefore.init.map { _ =>
        physicalColumns() should contain("legacyCamelField")
      }
    }
    "dispose the original instance" in {
      DBBefore.dispose.succeed
    }
    "re-initialize with the camelCase field removed, keeping its column under its real case" in {
      DBAfter.init.map { _ =>
        val cols = physicalColumns()
        cols should contain("legacyCamelField")
        cols should contain("keepField")
        DBAfter.records.asInstanceOf[lightdb.sql.SQLStore[?, ?]].retiredColumns shouldBe List("legacyCamelField")
      }
    }
    "drop the retired camelCase column when asked" in {
      DBAfter.records.asInstanceOf[lightdb.sql.SQLStore[?, ?]].dropRetiredColumns(List("legacyCamelField")).map { dropped =>
        dropped shouldBe List("legacyCamelField")
        physicalColumns() should not contain "legacyCamelField"
      }
    }
    "dispose" in {
      DBAfter.dispose.succeed
    }
  }

  // -- model "version A": carries the camelCase column --
  case class RecordWithLegacyColumn(keepField: String,
                                    legacyCamelField: String,
                                    created: Timestamp = Timestamp(),
                                    modified: Timestamp = Timestamp(),
                                    _id: Id[RecordWithLegacyColumn] = RecordWithLegacyColumn.id())
    extends RecordDocument[RecordWithLegacyColumn]

  object RecordWithLegacyColumn
    extends RecordDocumentModel[RecordWithLegacyColumn] with JsonConversion[RecordWithLegacyColumn] {
    override implicit val rw: RW[RecordWithLegacyColumn] = RW.gen
    val keepField: F[String]        = field("keepField", (d: RecordWithLegacyColumn) => d.keepField)
    val legacyCamelField: F[String] = field("legacyCamelField", (d: RecordWithLegacyColumn) => d.legacyCamelField)
  }

  // -- model "version B": same table, the camelCase column removed --
  case class RecordWithoutLegacyColumn(keepField: String,
                                       created: Timestamp = Timestamp(),
                                       modified: Timestamp = Timestamp(),
                                       _id: Id[RecordWithoutLegacyColumn] = RecordWithoutLegacyColumn.id())
    extends RecordDocument[RecordWithoutLegacyColumn]

  object RecordWithoutLegacyColumn
    extends RecordDocumentModel[RecordWithoutLegacyColumn] with JsonConversion[RecordWithoutLegacyColumn] {
    override implicit val rw: RW[RecordWithoutLegacyColumn] = RW.gen
    val keepField: F[String] = field("keepField", (d: RecordWithoutLegacyColumn) => d.keepField)
  }

  /** Database "version A" — its model still has `legacyCamelField`. */
  object DBBefore extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = PostgreSQLTestSupport.storeManager
    override def name: String = schema
    lazy val directory: Option[Path] = Some(Path.of(s"db/$schema"))
    val records: Collection[RecordWithLegacyColumn, RecordWithLegacyColumn.type] =
      store(RecordWithLegacyColumn).withName(table).apply()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  /** Database "version B" — same table, model no longer has the column.
    * Initializing this is the schema-evolution step that crashes. */
  object DBAfter extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = PostgreSQLTestSupport.storeManager
    override def name: String = schema
    lazy val directory: Option[Path] = Some(Path.of(s"db/$schema"))
    val records: Collection[RecordWithoutLegacyColumn, RecordWithoutLegacyColumn.type] =
      store(RecordWithoutLegacyColumn).withName(table).apply()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
