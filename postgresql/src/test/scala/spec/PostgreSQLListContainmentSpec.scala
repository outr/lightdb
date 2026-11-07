package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.filter.*
import lightdb.id.Id
import lightdb.sql.SQLStoreTransaction
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path
import java.sql.{Connection, DriverManager}

/**
 * List membership on PostgreSQL is jsonb containment, answered by a jsonb GIN index on each list field.
 *
 * A list is stored as its compact JSON text, and `has` was four ILIKE patterns per value over the column's trigram index: on a
 * copy of a 1.3M-row catalog, a 26-term "more like this" query ran 104 bitmap index scans and read 666k buffers in 859 ms, where
 * the same query as `("col")::jsonb @> '[id]'` over a `jsonb_path_ops` index ran 26 scans and 92k buffers in 131 ms, with the same
 * results. Containment matches a value exactly, as Lucene's term does, where ILIKE ignored case ("Foo" matched "foo").
 *
 * The index is `<Store>_<field>_jsonb`: not an `_idx` name, so the startup sweep leaves it alone, and `IF NOT EXISTS` takes one
 * built ahead of time (CONCURRENTLY, on a large table) under that name. The trigram index stays: matching inside a list's values
 * (starts with, ends with, exact) still uses LIKE.
 */
@EmbeddedTest
class PostgreSQLListContainmentSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with BeforeAndAfterAll with PostgreSQLAvailability {
  private val schema = "PostgreSQLListContainmentSpec"
  private val table = "Item"

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

  private def indexes(): Set[String] = withJdbc { c =>
    val ps = c.prepareStatement("SELECT indexname FROM pg_indexes WHERE schemaname = ? AND tablename = ?")
    try {
      ps.setString(1, schema)
      ps.setString(2, table)
      val rs = ps.executeQuery()
      Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toSet
    } finally ps.close()
  }

  /** The plan PostgreSQL chooses for `sql` when a sequential scan is ruled out, so a usable index shows on a small table. `sql` is
    * lightdb's debug rendering (queryLiteral), which writes a string inside a JSON value with its quotes escaped, so the plan is
    * checked on a list of numbers. */
  private def plan(sql: String): String = withJdbc { c =>
    val st = c.createStatement()
    try {
      st.execute("SET enable_seqscan = off")
      val rs = st.executeQuery(s"EXPLAIN $sql")
      Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).mkString("\n")
    } finally st.close()
  }

  private def ids(filter: Item.type => Filter[Item]): Task[Set[String]] =
    DB.items.transaction(_.query.filter(filter).toList).map(_.map(_._id.value).toSet)

  private val items = List(
    Item(List("foo", "bar"), List(5, 7), _id = Id("lower")),
    Item(List("Foo"), List(15, 50, 51), _id = Id("upper")),
    Item(Nil, Nil, _id = Id("empty"))
  )

  "List membership on PostgreSQL" should {
    "initialize the database" in {
      DB.init.succeed
    }
    "create a jsonb index for each list field, beside its trigram index" in {
      indexes() should contain allOf(s"${table}_tags_jsonb", s"${table}_scores_jsonb", s"${table}_tags_idx", s"${table}_scores_idx")
    }
    "insert the items" in {
      DB.items.transaction(_.insert(items)).succeed
    }
    "match a value exactly, not inside a longer one or in another case" in {
      for {
        five <- ids(_.scores has 5)
        foo <- ids(_.tags has "foo")
        upper <- ids(_.tags has "Foo")
        both <- ids(_.tags === List("foo", "bar"))
      } yield {
        five should be(Set("lower"))
        foo should be(Set("lower"))
        upper should be(Set("upper"))
        both should be(Set("lower"))
      }
    }
    "exclude a row that holds any of the values a not-equals names" in {
      ids(_.scores !== List(5, 51)).map(_ should be(Set("empty")))
    }
    "answer membership through the jsonb index" in {
      DB.items.transaction { tx =>
        Task(tx.asInstanceOf[SQLStoreTransaction[Item, Item.type]].toSQL(tx.query.filter(_.scores has 7)).query.queryLiteral)
      }.map { sql =>
        sql should include("::jsonb @>")
        plan(sql) should include(s"${table}_scores_jsonb")
      }
    }
    "open again over the indexes it made" in {
      for {
        _ <- DB.dispose
        _ <- DB2.init
        found <- DB2.items.transaction(_.query.filter(_.scores has 7).toList).map(_.map(_._id.value).toSet)
        _ <- DB2.truncate()
        _ <- DB2.dispose
      } yield found should be(Set("lower"))
    }
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = PostgreSQLTestSupport.storeManager
    override def name: String = schema
    lazy val directory: Option[Path] = None
    val items: Collection[Item, Item.type] = store(Item).withName(table)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  object DB2 extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = PostgreSQLTestSupport.storeManager
    override def name: String = schema
    lazy val directory: Option[Path] = None
    val items: Collection[Item, Item.type] = store(Item).withName(table)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Item(tags: List[String],
                  scores: List[Int],
                  created: Timestamp = Timestamp(),
                  modified: Timestamp = Timestamp(),
                  _id: Id[Item] = Item.id()) extends RecordDocument[Item]

  object Item extends RecordDocumentModel[Item] with JsonConversion[Item] {
    override implicit val rw: RW[Item] = RW.gen
    val tags: I[List[String]] = field.index("tags", (i: Item) => i.tags)
    val scores: I[List[Int]] = field.index("scores", (i: Item) => i.scores)
  }
}
