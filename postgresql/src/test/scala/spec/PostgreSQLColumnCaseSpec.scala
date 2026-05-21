package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.postgresql.PostgreSQLStoreManager
import lightdb.store.{Collection, CollectionManager}
import lightdb.sql.connect.{HikariConnectionManager, SQLConfig}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path
import java.sql.{Connection, DriverManager}

/**
 * Reproduces the column-name casing bug in `SQLStore`'s schema
 * reconciliation.
 *
 * LightDB quotes every identifier it emits (`SqlIdent.quote`), so a
 * model field `legacyCamelField` becomes a Postgres column physically
 * named `legacyCamelField` — case preserved. But `SQLStore.columns()`
 * reads catalog names back through
 * `ResultSetMetaData.getColumnName(...).toLowerCase`, and the drop path
 * (`initTransaction`) then feeds that lowercased name straight into a
 * *quoted* `DROP COLUMN "..."`. Quoted identifiers are case-sensitive,
 * so `DROP COLUMN "legacycamelfield"` does NOT match the real
 * `legacyCamelField` column — Postgres rejects it with
 * `column "legacycamelfield" ... does not exist` and store
 * initialization crashes.
 *
 * Real-world impact: deploy version A of a model, then version B with a
 * camelCase field removed — version B's startup reconciliation cannot
 * drop the now-unused column and the process fails to boot.
 *
 * Test shape: initialize the table with the camelCase column present,
 * dispose, then initialize a second instance whose model lacks that
 * column. The second initialization must succeed and the column must
 * actually be gone.
 */
@EmbeddedTest
class PostgreSQLColumnCaseSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with BeforeAndAfterAll {
  private val schema = "PostgreSQLColumnCaseSpec"
  private val table  = "schema_evolution_record"

  private val jdbcUrl  = "jdbc:postgresql://localhost:5432/basic"
  private val jdbcUser = "postgres"
  private val jdbcPass = "password"

  private def sqlConfig: SQLConfig = SQLConfig(
    jdbcUrl  = jdbcUrl,
    username = Some(jdbcUser),
    password = Some(jdbcPass)
  )

  private def withJdbc[T](f: Connection => T): T = {
    val c = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)
    try f(c) finally c.close()
  }

  /** Drop the test schema so every run starts from a clean slate. */
  override def beforeAll(): Unit = {
    super.beforeAll()
    withJdbc { c =>
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
    "re-initialize with the camelCase column removed — reconciliation must drop it" in {
      // Before the SQLStore fix this throws:
      //   RuntimeException: Failed to execute update:
      //     ALTER TABLE "PostgreSQLColumnCaseSpec"."schema_evolution_record"
      //     DROP COLUMN "legacycamelfield"
      //   Caused by: column "legacycamelfield" ... does not exist
      DBAfter.init.map { _ =>
        val cols = physicalColumns()
        cols should not contain "legacyCamelField"
        cols should contain("keepField")
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
    override val storeManager: CollectionManager = PostgreSQLStoreManager(HikariConnectionManager(sqlConfig))
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
    override val storeManager: CollectionManager = PostgreSQLStoreManager(HikariConnectionManager(sqlConfig))
    override def name: String = schema
    lazy val directory: Option[Path] = Some(Path.of(s"db/$schema"))
    val records: Collection[RecordWithoutLegacyColumn, RecordWithoutLegacyColumn.type] =
      store(RecordWithoutLegacyColumn).withName(table).apply()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
