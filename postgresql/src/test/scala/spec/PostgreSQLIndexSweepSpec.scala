package spec

import fabric.rw.*
import lightdb.{CompositeIndex, LightDB}
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.field.Field
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
 * The startup index sweep on PostgreSQL.
 *
 * `SQLStore.initTransaction` drops indexes named by LightDB's convention (`<Store>_<column>_idx`)
 * whose column is no longer a model field. Two things were wrong with it on PostgreSQL:
 *
 *   - A composite index is named `<Store>_<indexName>_idx` and a tokenized field's trigram index
 *     `<Store>_<field>_trgm_idx`. Neither `<indexName>` nor `trgm` is a column, so the sweep flagged
 *     LightDB's own indexes on every startup.
 *   - The catalog listing was lowercased and the DROP unquoted, so PostgreSQL folded the name to one
 *     that does not exist and `IF EXISTS` turned every drop into a silent no-op. That hid the first
 *     bug, and it also meant a genuinely stale index was never removed.
 *
 * Test shape: initialize once (LightDB builds its indexes), dispose, add a stale mixed-case index and
 * a hand-made column-backed index, initialize again. The declared composite and trigram indexes and
 * the column-backed one must survive; the stale one must actually be gone.
 */
@EmbeddedTest
class PostgreSQLIndexSweepSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with BeforeAndAfterAll with PostgreSQLAvailability {
  private val schema = "PostgreSQLIndexSweepSpec"
  private val table = "SweepRecord"

  private def withJdbc[T](f: Connection => T): T = {
    val c = DriverManager.getConnection(PostgreSQLTestSupport.jdbcUrl.get, PostgreSQLTestSupport.username, PostgreSQLTestSupport.password)
    try f(c) finally c.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if PostgreSQLTestSupport.jdbcUrl.isDefined then withJdbc { c =>
      val st = c.createStatement()
      try st.executeUpdate(s"""DROP SCHEMA IF EXISTS "$schema" CASCADE""")
      finally st.close()
    }
  }

  /** Index names on the test table exactly as PostgreSQL stores them (case preserved). */
  private def physicalIndexes(): Set[String] = withJdbc { c =>
    val ps = c.prepareStatement("SELECT indexname FROM pg_indexes WHERE schemaname = ? AND tablename = ?")
    try {
      ps.setString(1, schema)
      ps.setString(2, table)
      val rs = ps.executeQuery()
      val b = Set.newBuilder[String]
      while rs.next() do b += rs.getString(1)
      b.result()
    } finally ps.close()
  }

  private val fieldIndex = s"${table}_keepField_idx"
  private val compositeIndex = s"${table}_keepTitle_idx"
  private val trigramIndex = s"${table}_title_trgm_idx"
  private val staleIndex = s"${table}_legacyThing_idx"
  private val handMadeIndex = "byHand_keepField_idx"

  "PostgreSQLIndexSweepSpec" should {
    "create the declared field, composite, and trigram indexes" in {
      DBFirst.init.map { _ =>
        physicalIndexes() should contain allOf (fieldIndex, compositeIndex, trigramIndex)
      }
    }
    "dispose the first instance" in {
      DBFirst.dispose.succeed
    }
    "add a stale mixed-case index and a hand-made column-backed index" in {
      withJdbc { c =>
        val st = c.createStatement()
        try {
          st.executeUpdate(s"""CREATE INDEX "$staleIndex" ON "$schema"."$table" ("keepField")""")
          st.executeUpdate(s"""CREATE INDEX "$handMadeIndex" ON "$schema"."$table" ("keepField")""")
        } finally st.close()
      }
      physicalIndexes() should contain allOf (staleIndex, handMadeIndex)
    }
    "re-initialize: keep declared and column-backed indexes, drop only the stale one" in {
      DBSecond.init.map { _ =>
        val indexes = physicalIndexes()
        indexes should contain allOf (fieldIndex, compositeIndex, trigramIndex, handMadeIndex)
        indexes should not contain staleIndex
      }
    }
    "dispose" in {
      DBSecond.dispose.succeed
    }
  }

  case class SweepRecord(keepField: String,
                         title: String,
                         created: Timestamp = Timestamp(),
                         modified: Timestamp = Timestamp(),
                         _id: Id[SweepRecord] = ModelFirst.id())
    extends RecordDocument[SweepRecord]

  /** One model shape, instantiated once per database, so the second initialization sees exactly the
    * declarations the first one built from. */
  class SweepModel extends RecordDocumentModel[SweepRecord] with JsonConversion[SweepRecord] {
    override implicit val rw: RW[SweepRecord] = RW.gen
    val keepField: Field.Indexed[SweepRecord, String] = field.index("keepField", (d: SweepRecord) => d.keepField)
    val title: Field.Tokenized[SweepRecord] = field.tokenized("title", (d: SweepRecord) => d.title)
    val keepTitle: CompositeIndex[SweepRecord] = field.indexComposite(List(keepField, title))
  }

  object ModelFirst extends SweepModel
  object ModelSecond extends SweepModel

  object DBFirst extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = PostgreSQLTestSupport.storeManager
    override def name: String = schema
    lazy val directory: Option[Path] = Some(Path.of(s"db/$schema"))
    val records: Collection[SweepRecord, ModelFirst.type] = store(ModelFirst).withName(table).apply()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  object DBSecond extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = PostgreSQLTestSupport.storeManager
    override def name: String = schema
    lazy val directory: Option[Path] = Some(Path.of(s"db/$schema"))
    val records: Collection[SweepRecord, ModelSecond.type] = store(ModelSecond).withName(table).apply()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
