package spec

import lightdb.KeyValue
import lightdb.rocksdb.RocksDBStore
import lightdb.traversal.graph.DisjointSet
import lightdb.upgrade.DatabaseUpgrade
import lightdb.LightDB
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

@EmbeddedTest
class RocksDBKeyValueDisjointSetSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  object DB extends LightDB {
    override type SM = RocksDBStore.type
    override val storeManager: RocksDBStore.type = RocksDBStore
    override lazy val directory: Option[Path] = Some(Path.of("db/RocksDBKeyValueDisjointSetSpec"))
    override def upgrades: List[DatabaseUpgrade] = Nil

    val kv: S[KeyValue, KeyValue.type] = store(KeyValue)
  }

  "KeyValueDisjointSet" should {
    "union and find through a KeyValue-backed store transaction" in {
      DB.init.next {
        DB.kv.transaction { tx =>
          val ds = DisjointSet.keyValue(tx, namespace = "test")
          for {
            _ <- ds.union("a", "b")
            _ <- ds.union("b", "c")
            _ <- ds.union("d", "e")
            ra <- ds.find("a")
            rc <- ds.find("c")
            rd <- ds.find("d")
            re <- ds.find("e")
          } yield {
            ra shouldBe rc
            rd shouldBe re
            ra should not be rd
          }
        }
      }.guarantee(DB.dispose).succeed
    }
  }
}

