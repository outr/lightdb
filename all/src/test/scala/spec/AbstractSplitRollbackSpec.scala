package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.lucene.{LuceneDurability, LuceneStore, LuceneStoreManager}
import lightdb.rocksdb.RocksDBStore
import lightdb.store.split.{SplitCollection, SplitStoreManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.*

import java.nio.file.{Files, Path}
import java.util.Comparator

/**
 * A rolled-back transaction on a split collection over RocksDB that writes through (the default) discards its writes:
 * storage holds no row it wrote and every row it overwrote or deleted is back, the index matches, and another open
 * transaction's index changes survive the rollback.
 */
abstract class AbstractSplitRollbackSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  import AbstractSplitRollbackSpec.*

  protected def durability: LuceneDurability

  private lazy val root = Path.of(s"db/${getClass.getSimpleName}")

  private lazy val db: DB = {
    if Files.exists(root) then Files.walk(root).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))
    new DB(root, durability)
  }

  private def stored(id: String): Task[Option[String]] = db.items.transaction(_.storage.get(Id[Item](id)).map(_.map(_.name)))

  private def indexed(name: String): Task[Int] = db.items.transaction(_.query.filter(_.name === name).count)

  private def aborted(f: db.items.TX => Task[Unit]): Task[Boolean] =
    db.items.transaction(tx => f(tx).next(Task.error[Unit](new RuntimeException("abort")))).attempt.map(_.isFailure)

  "A write-through split collection" should {
    "initialize" in db.init.succeed
    "leave no storage row and no index row after a rollback" in {
      for
        failed <- aborted(tx => tx.insert(Item("gone", Id("gone"))).unit)
        row <- stored("gone")
        hits <- indexed("gone")
      yield {
        failed shouldBe true
        row shouldBe None
        hits shouldBe 0
      }
    }
    "put back the rows a rolled-back transaction overwrote and deleted" in {
      for
        _ <- db.items.transaction(_.insert(List(Item("original", Id("o")), Item("deleted", Id("d")))))
        failed <- aborted(tx => tx.upsert(Item("changed", Id("o"))).next(tx.delete(Id[Item]("d"))).unit)
        o <- stored("o")
        d <- stored("d")
        original <- indexed("original")
        changed <- indexed("changed")
        deleted <- indexed("deleted")
      yield {
        failed shouldBe true
        o shouldBe Some("original")
        d shouldBe Some("deleted")
        (original, changed, deleted) shouldBe (1, 0, 1)
      }
    }
    "keep another open transaction's index rows through a rollback" in {
      for
        open <- db.items.transaction.create()
        _ <- open.insert(Item("kept", Id("kept")))
        failed <- aborted(tx => tx.insert(Item("dropped", Id("dropped"))).unit)
        _ <- db.items.transaction.release(open)
        kept <- indexed("kept")
        dropped <- indexed("dropped")
        keptRow <- stored("kept")
      yield {
        failed shouldBe true
        keptRow shouldBe Some("kept")
        kept shouldBe 1
        dropped shouldBe 0
      }
    }
    "dispose" in db.dispose.succeed
  }
}

object AbstractSplitRollbackSpec {
  case class Item(name: String, _id: Id[Item] = Item.id()) extends Document[Item]

  object Item extends DocumentModel[Item] with JsonConversion[Item] {
    override implicit val rw: RW[Item] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  class DB(dir: Path, durability: LuceneDurability) extends LightDB {
    override type SM = SplitStoreManager[RocksDBStore.type, LuceneStoreManager]
    override val storeManager: SM = SplitStoreManager(RocksDBStore, LuceneStore.withDurability(durability))
    override def name: String = "SplitRollback"
    override lazy val directory: Option[Path] = Some(dir)
    override protected def disposeOnShutdown: Boolean = false
    val items: SplitCollection[Item, Item.type, RocksDBStore[Item, Item.type], LuceneStore[Item, Item.type]] = store(Item)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
