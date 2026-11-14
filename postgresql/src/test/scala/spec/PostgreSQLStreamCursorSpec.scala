package spec

import fabric.rw.*
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.postgresql.{PostgreSQLStore, PostgreSQLStoreManager}
import lightdb.sql.SQLStoreTransaction
import lightdb.sql.connect.HikariConnectionManager
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

/**
 * A stream longer than the fetch size reads its rows through one server-side portal, a fetch of rows at a time: from
 * its first row into its second fetch the same portal, for the whole query without a LIMIT, is open on the
 * transaction's connection, and every row comes from it in order. The same holds when the connection begins its
 * transaction lazily.
 */
@EmbeddedTest
class PostgreSQLStreamCursorSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with PostgreSQLAvailability {
  private val rows = SQLStoreTransaction.FetchSize * 2 + 500

  case class Item(n: Int,
                  created: Timestamp = Timestamp(),
                  modified: Timestamp = Timestamp(),
                  _id: Id[Item] = Item.id()) extends RecordDocument[Item]

  object Item extends RecordDocumentModel[Item] with JsonConversion[Item] {
    override implicit val rw: RW[Item] = RW.gen
    val n: I[Int] = field.index(_.n)
  }

  class DB(schema: String, lazyBegin: Boolean) extends LightDB {
    override type SM = PostgreSQLStoreManager
    override val storeManager: PostgreSQLStoreManager =
      PostgreSQLStoreManager(HikariConnectionManager(PostgreSQLTestSupport.sqlConfig.copy(lazyBegin = lazyBegin)))
    override def name: String = schema
    lazy val directory: Option[Path] = Some(Path.of(s"db/$schema"))
    val items: PostgreSQLStore[Item, Item.type] = store(Item)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  /** The stream's values and the statements of the portals open on its connection at its first row and at the first row of its second fetch. */
  private def streamed(db: DB): Task[(List[Int], List[String])] = db.items.transaction { tx =>
    var portals = List.empty[String]
    tx.query.sort(Sort.ByField(Item.n).ascending).stream.zipWithIndex.map {
      case (item, index) =>
        if index == 0 || index == SQLStoreTransaction.FetchSize then {
          val st = tx.state.openConnection().createStatement()
          try {
            val rs = st.executeQuery("SELECT statement FROM pg_cursors WHERE statement NOT LIKE '%pg_cursors%'")
            portals = portals ::: Iterator.continually(rs).takeWhile(_.next()).map(_.getString(1)).toList
          } finally st.close()
        }
        item.n
    }.toList.map(values => (values, portals))
  }

  private def check(db: DB) = for {
    _ <- db.init
    _ <- db.items.transaction(_.truncate)
    _ <- db.items.transaction(_.insert((1 to rows).toList.map(i => Item(i, _id = Id(s"item$i")))))
    (values, portals) <- streamed(db)
    _ <- db.items.transaction(_.truncate)
    _ <- db.dispose
  } yield {
    values shouldBe (1 to rows).toList
    portals.size shouldBe 2
    portals.distinct.size shouldBe 1
    portals.head should not include "LIMIT"
    portals.head should include("ORDER BY")
  }

  "PostgreSQL streaming" should {
    "read a stream longer than the fetch size through one open portal" in check(new DB("PostgreSQLStreamCursorSpec", lazyBegin = false))
    "read it through one open portal when the transaction begins lazily" in check(new DB("PostgreSQLStreamCursorLazySpec", lazyBegin = true))
  }
}
