package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.sql.SQLiteStore
import lightdb.store.{Collection, CollectionManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.{Files, Path}
import java.util.Comparator

/**
 * Regression: model field names that collide with SQL reserved words must work end-to-end.
 *
 * Always-quoting in `SqlIdent` is what makes this safe — if a CREATE / INSERT / SELECT path
 * ever reverts to bare-string interpolation of `field.name`, this spec will fail with a
 * `near "limit": syntax error` (or the dialect's equivalent).
 */
@EmbeddedTest
class SQLiteReservedWordFieldsSpec extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers {

  // Field names chosen from the SQL standard reserved-word list. All real model field names
  // we'd never call this — but a user can, and it must work.
  case class Tricky(limit: Int,
                    order: String,
                    `select`: Boolean,
                    `from`: String,
                    `where`: String,
                    `group`: Int,
                    _id: Id[Tricky] = Tricky.id()) extends Document[Tricky]

  object Tricky extends DocumentModel[Tricky] with JsonConversion[Tricky] {
    override implicit val rw: RW[Tricky] = RW.gen
    val limit: I[Int] = field.index("limit", _.limit)
    val order: I[String] = field.index("order", _.order)
    val select: I[Boolean] = field.index("select", _.`select`)
    val from: I[String] = field.index("from", _.`from`)
    val where: I[String] = field.index("where", _.`where`)
    val group: I[Int] = field.index("group", _.`group`)
  }

  private val specName = getClass.getSimpleName
  private val dbPath: Path = Path.of(s"db/$specName")
  if Files.exists(dbPath) then {
    Files.walk(dbPath).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = SQLiteStore
    override def name: String = specName
    override lazy val directory: Option[Path] = Some(dbPath)
    val tricky: Collection[Tricky, Tricky.type] = store(Tricky)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  private val first = Tricky(limit = 10, order = "asc", `select` = true, `from` = "x", `where` = "y", `group` = 1)
  private val second = Tricky(limit = 20, order = "desc", `select` = false, `from` = "z", `where` = "w", `group` = 2)

  "SQLiteReservedWordFieldsSpec" should {
    "initialize without 'near limit: syntax error'" in {
      DB.init.succeed
    }
    "insert rows with reserved-word column names" in {
      DB.tricky.transaction(_.insert(List(first, second))).map(_.size should be(2))
    }
    "round-trip via _id lookup" in {
      DB.tricky.transaction { tx =>
        tx(first._id).map { row =>
          row.limit should be(10)
          row.order should be("asc")
          row.`select` should be(true)
          row.`from` should be("x")
          row.`where` should be("y")
          row.`group` should be(1)
        }
      }
    }
    "filter by a reserved-word indexed field" in {
      DB.tricky.transaction { tx =>
        tx.query.filter(_.limit === 20).toList.map { rows =>
          rows.map(_.limit) should be(List(20))
        }
      }
    }
    "sort by a reserved-word indexed field" in {
      DB.tricky.transaction { tx =>
        tx.query.sort(lightdb.Sort.ByField(Tricky.order)).toList.map { rows =>
          rows.map(_.order) should be(List("asc", "desc"))
        }
      }
    }
    "dispose the database" in {
      DB.dispose.succeed
    }
  }
}
