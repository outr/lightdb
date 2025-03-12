package spec

import fabric.rw._
import lightdb._
import lightdb.collection.Collection
import lightdb.doc._
import lightdb.field.Field
import lightdb.store.{MapStore, StoreManager}
import lightdb.store.sharded.ShardedStoreManager
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

@EmbeddedTest
class ShardedStorePaginationSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  // Create a model for a simple document
  case class TestDoc(value: Int, _id: Id[TestDoc] = Id[TestDoc]()) extends Document[TestDoc]

  object TestDoc extends DocumentModel[TestDoc] with JsonConversion[TestDoc] {
    override implicit val rw: RW[TestDoc] = RW.gen

    val value: Field.Indexed[TestDoc, Int] = field.index("value", (d: TestDoc) => d.value)
  }

  // Create a database with a collection of TestDocs
  class DB extends LightDB {
    override lazy val directory: Option[Path] = Some(Path.of(s"db/ShardedStorePaginationSpec"))

    val docs: Collection[TestDoc, TestDoc.type] = collection(TestDoc)

    override def storeManager: StoreManager = ShardedStoreManager(MapStore, 3)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  protected var db: DB = new DB

  "ShardedStore" should {
    "initialize the database" in {
      db.init.succeed
    }

    "properly handle pagination when merging results from multiple shards" in {
      // Create 300 documents (100 per shard)
      val docs = (1 to 300).map(i => TestDoc(value = i)).toList

      // Insert the documents
      db.docs.transaction { implicit transaction =>
        db.docs.insert(docs).flatMap { _ =>
          // Verify that all documents were inserted
          db.docs.count.flatMap { count =>
            count should be(300)

            // Query with a limit of 100
            db.docs.query.sort(Sort.ByField(db.docs.model.value, SortDirection.Ascending)).limit(100).toList.map { results =>
              // Verify that we got 100 results
              results.size should be(100)

              // Verify that we got the first 100 documents (values 1-100)
              results.map(_.value).sorted should be((1 to 100).toList)
            }
          }
        }
      }
    }

    "properly handle offset when merging results from multiple shards" in {
      db.docs.transaction { implicit transaction =>
        // Query with an offset of 100 and a limit of 100
        db.docs.query.sort(Sort.ByField(db.docs.model.value, SortDirection.Ascending)).offset(100).limit(100).toList.map { results =>
          // Verify that we got 100 results
          results.size should be(100)

          // Verify that we got the second 100 documents (values 101-200)
          results.map(_.value).sorted should be((101 to 200).toList)
        }
      }
    }

    "properly handle sorting when merging results from multiple shards" in {
      db.docs.transaction { implicit transaction =>
        // Query with descending sort
        db.docs.query.sort(Sort.ByField(db.docs.model.value, SortDirection.Descending)).limit(100).toList.map { results =>
          // Verify that we got 100 results
          results.size should be(100)

          // Verify that we got the last 100 documents (values 201-300)
          results.map(_.value).sorted.reverse should be((201 to 300).toList)
        }
      }
    }

    "dispose the database" in {
      db.dispose.succeed
    }
  }
}
