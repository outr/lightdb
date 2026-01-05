package spec

import lightdb.KeyValue
import lightdb.rocksdb.RocksDBStore
import lightdb.traversal.graph.DisjointSet
import lightdb.upgrade.DatabaseUpgrade
import lightdb.LightDB
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec
import rapid.Task

import java.nio.file.Files
import java.nio.file.Path

@EmbeddedTest
class RocksDBKeyValueDisjointSetSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  private class DB(dir: Path) extends LightDB {
    override type SM = RocksDBStore.type
    override val storeManager: RocksDBStore.type = RocksDBStore
    override lazy val directory: Option[Path] = Some(dir)
    override def upgrades: List[DatabaseUpgrade] = Nil

    val kv: S[KeyValue, KeyValue.type] = store(KeyValue, name = Some("KeyValue"))
  }

  "KeyValueDisjointSet" should {
    "union within a transaction" in {
      val test = for {
        dir <- Task(Files.createTempDirectory("lightdb-ds-rocksdb-"))
        db = new DB(dir)
        _ <- db.init
        out <- db.kv.transaction { tx =>
          val ds = DisjointSet.keyValue(tx, namespace = "t1")
          for {
            _ <- ds.union("a", "b")
            _ <- ds.union("b", "c")
            ra <- ds.find("a")
            rc <- ds.find("c")
          } yield ra -> rc
        }
        _ <- db.dispose
      } yield out

      test.map { case (ra, rc) => ra shouldBe rc }.succeed
    }

    "preserve unions across transactions (same backing store)" in {
      val test = for {
        dir <- Task(Files.createTempDirectory("lightdb-ds-rocksdb-"))
        db = new DB(dir)
        _ <- db.init
        _ <- db.kv.transaction { tx =>
          val ds = DisjointSet.keyValue(tx, namespace = "t2")
          ds.union("a", "b").next(ds.union("b", "c"))
        }
        roots <- db.kv.transaction { tx =>
          val ds = DisjointSet.keyValue(tx, namespace = "t2")
          ds.find("a").flatMap(ra => ds.find("c").map(rc => ra -> rc))
        }
        _ <- db.dispose
      } yield roots

      test.map { case (ra, rc) => ra shouldBe rc }.succeed
    }

    "isolate namespaces" in {
      val test = for {
        dir <- Task(Files.createTempDirectory("lightdb-ds-rocksdb-"))
        db = new DB(dir)
        _ <- db.init
        out <- db.kv.transaction { tx =>
          val ds1 = DisjointSet.keyValue(tx, namespace = "ns1")
          val ds2 = DisjointSet.keyValue(tx, namespace = "ns2")
          for {
            _ <- ds1.union("a", "b")
            r1a <- ds1.find("a")
            r1b <- ds1.find("b")
            r2a <- ds2.find("a")
            r2b <- ds2.find("b")
          } yield (r1a, r1b, r2a, r2b)
        }
        _ <- db.dispose
      } yield out

      test.map { case (r1a, r1b, r2a, r2b) =>
        r1a shouldBe r1b
        r2a should not be r2b
      }.succeed
    }
  }
}

