package lightdb.rocksdb

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.store.split.{SplitCollection, SplitStoreManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.*
import spec.EmbeddedTest

import java.nio.file.{Files, Path}
import java.util.Comparator

/**
 * A split transaction over RocksDB records undo on its first write. A bulk upsert after that keeps RocksDB's batched
 * write path and does not read or hold the rows it overwrites (a snapshot keeps them readable), and a rollback still puts every prior row back exactly:
 * overwritten rows byte for byte, new rows gone, the index matching storage.
 */
@EmbeddedTest
class SplitUndoBatchUpsertSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  import SplitUndoBatchUpsertSpec.*

  private val N = 10_000
  private val half = N / 2

  private lazy val root = Path.of(s"db/${getClass.getSimpleName}")

  private lazy val db: DB = {
    if Files.exists(root) then Files.walk(root).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))
    new DB(root)
  }

  private def existing(prefix: String): List[Item] = (0 until half).toList.map(i => Item(s"old-$i", Id(s"$prefix-$i")))
  private def batch(prefix: String): List[Item] =
    (0 until half).toList.map(i => Item(s"new-$i", Id(s"$prefix-$i"))) ++
      (0 until half).toList.map(i => Item(s"new-$i", Id(s"$prefix-added-$i")))

  private def raw(id: String): Option[Array[Byte]] = {
    val s = db.items.storage
    Option(s.native(s.handle match {
      case Some(h) => s.rocksDB.get(h, Id[Item](id).bytes)
      case None => s.rocksDB.get(Id[Item](id).bytes)
    }))
  }

  private def rawRows(prefix: String): List[Option[List[Byte]]] =
    (0 until half).toList.map(i => raw(s"$prefix-$i").map(_.toList))

  private def indexed(prefix: String): Task[Int] = db.items.transaction(_.query.filter(_.name startsWith prefix).count)

  private case class Seen(undoReads: Long, undoRowsHeld: Int, undoSnapshots: Int, batchedRows: Long, streamBatchedRows: Long, millis: Long)

  private def seen(tx: db.items.TX, start: Long): Seen = Seen(
    undoReads = tx.storage.undoReads.get(),
    undoRowsHeld = tx.storage.undoRowsHeld,
    undoSnapshots = tx.storage.undoSnapshotsHeld,
    batchedRows = tx.storage.batchedRows.get(),
    streamBatchedRows = tx.storage.streamBatchedRows.get(),
    millis = (System.nanoTime() - start) / 1_000_000L
  )

  // Upserts `docs` through `write` after a first write has recorded undo, notes the counters, then fails the
  // transaction so it rolls back.
  private def rolledBack(write: db.items.TX => Task[Int]): Task[Seen] = {
    @volatile var result: Option[Seen] = None
    db.items.transaction { tx =>
      tx.upsert(Item("first", Id("first"))).flatMap { _ =>
        val start = System.nanoTime()
        write(tx).flatMap { count =>
          count shouldBe N
          result = Some(seen(tx, start))
          Task.error[Unit](new RuntimeException("roll back"))
        }
      }
    }.attempt.map { r =>
      r.isFailure shouldBe true
      result.getOrElse(fail("the write did not complete"))
    }
  }

  private def restored(prefix: String, before: List[Option[List[Byte]]]): Task[Unit] = for
    after <- Task(rawRows(prefix))
    added <- Task((0 until half).count(i => raw(s"$prefix-added-$i").nonEmpty))
    first <- Task(raw("first"))
    oldHits <- indexed("old-")
    newHits <- indexed("new-")
    firstHits <- indexed("first")
  yield {
    after.count(_.isEmpty) shouldBe 0
    after shouldBe before
    added shouldBe 0
    first shouldBe None
    (oldHits, newHits, firstHits) shouldBe (2 * half, 0, 0)
  }

  "A split transaction over RocksDB that has recorded undo" should {
    "initialize" in db.init.next(db.items.transaction(_.insert(existing("split") ++ existing("storage")))).succeed
    "upsert a stream without reading or holding the rows it overwrites, and roll back exactly" in {
      val before = rawRows("split")
      for
        s <- rolledBack(tx => tx.upsert(rapid.Stream.emits(batch("split"))))
        _ <- logger.info(s"split upsert(stream) of $N rows after undo: ${s.millis} ms, $s")
        _ <- restored("split", before)
      yield {
        s.undoReads shouldBe 0L
        s.undoRowsHeld shouldBe 0
        s.undoSnapshots shouldBe 1
        s.batchedRows shouldBe (N + 1).toLong
      }
    }
    "keep the storage stream upsert on its WriteBatch path, and roll back exactly" in {
      val before = rawRows("storage")
      for
        s <- rolledBack(tx => tx.storage.upsert(rapid.Stream.emits(batch("storage"))))
        _ <- logger.info(s"storage upsert(stream) of $N rows after undo: ${s.millis} ms, $s")
        // The storage stream wrote around the index; the rollback restores the index rows it touched from storage.
        _ <- restored("storage", before)
      yield {
        s.undoReads shouldBe 0L
        s.undoRowsHeld shouldBe 0
        s.undoSnapshots shouldBe 1
        s.streamBatchedRows shouldBe N.toLong
      }
    }
    "commit a stream upsert" in {
      for
        count <- db.items.transaction(tx => tx.upsert(Item("first", Id("first"))).next(tx.upsert(rapid.Stream.emits(batch("split")))))
        row <- db.items.transaction(_.get(Id[Item]("split-added-7")))
        old <- db.items.transaction(_.get(Id[Item]("split-7")))
        newHits <- indexed("new-")
      yield {
        count shouldBe N
        row.map(_.name) shouldBe Some("new-7")
        old.map(_.name) shouldBe Some("new-7")
        newHits shouldBe N
      }
    }
    "put back, after a truncate, the rows the transaction wrote before it, as a rolled-back truncate always has" in {
      for
        _ <- db.scratch.transaction(_.insert(List(Item("kept-a", Id("a")), Item("kept-b", Id("b")), Item("kept-c", Id("c")))))
        failed <- db.scratch.transaction { tx =>
          tx.upsert(rapid.Stream.emits(List(Item("changed-a", Id("a")), Item("added-d", Id("d")))))
            .next(tx.truncate)
            .next(tx.upsert(Item("added-e", Id("e"))))
            .next(Task.error[Unit](new RuntimeException("roll back")))
        }.attempt.map(_.isFailure)
        rows <- db.scratch.transaction(_.storage.list)
      yield {
        failed shouldBe true
        rows.map(i => i._id.value -> i.name).sorted shouldBe List("a" -> "kept-a")
      }
    }
    "put back the row another transaction committed between this one's writes, not the one before it" in {
      for
        _ <- db.scratch.transaction(_.insert(List(Item("before-x", Id("x")), Item("before-y", Id("y")))))
        failed <- db.scratch.transaction { tx =>
          tx.upsert(Item("mine-x", Id("x")))
            .next(db.scratch.transaction(_.upsert(Item("theirs-y", Id("y")))))
            .next(tx.upsert(rapid.Stream.emits(List(Item("mine-y", Id("y")), Item("mine-z", Id("z"))))))
            .next(Task.error[Unit](new RuntimeException("roll back")))
        }.attempt.map(_.isFailure)
        rows <- db.scratch.transaction(_.storage.list)
        theirs <- db.scratch.transaction(_.query.filter(_.name === "theirs-y").count)
        mine <- db.scratch.transaction(_.query.filter(_.name startsWith "mine-").count)
      yield {
        failed shouldBe true
        rows.map(i => i._id.value -> i.name).sorted shouldBe List("a" -> "kept-a", "x" -> "before-x", "y" -> "theirs-y")
        (theirs, mine) shouldBe (1, 0)
      }
    }
    "dispose" in db.dispose.succeed
  }
}

object SplitUndoBatchUpsertSpec {
  case class Item(name: String, _id: Id[Item] = Item.id()) extends Document[Item]

  object Item extends DocumentModel[Item] with JsonConversion[Item] {
    override implicit val rw: RW[Item] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  class DB(dir: Path) extends LightDB {
    override type SM = SplitStoreManager[RocksDBStore.type, LuceneStore.type]
    override val storeManager: SM = SplitStoreManager(RocksDBStore, LuceneStore)
    override def name: String = "SplitUndoBatchUpsert"
    override lazy val directory: Option[Path] = Some(dir)
    override protected def disposeOnShutdown: Boolean = false
    val items: SplitCollection[Item, Item.type, RocksDBStore[Item, Item.type], LuceneStore[Item, Item.type]] = store(Item)()
    val scratch: SplitCollection[Item, Item.type, RocksDBStore[Item, Item.type], LuceneStore[Item, Item.type]] = store(Item).withName("scratch")()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
