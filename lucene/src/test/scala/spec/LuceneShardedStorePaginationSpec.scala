// TODO: Revisit!
//package spec
//
//import fabric.rw._
//import lightdb._
//import lightdb.doc._
//import lightdb.field.Field
//import lightdb.lucene.LuceneStore
//import lightdb.store.Collection
//import lightdb.store.sharded.manager.BalancedShardManager
//import lightdb.store.sharded.{ShardedStore, ShardedStoreManager}
//import lightdb.upgrade.DatabaseUpgrade
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AsyncWordSpec
//import rapid.AsyncTaskSpec
//
//import java.nio.file.Path
//
//@EmbeddedTest
//class LuceneShardedStorePaginationSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
//  "ShardedStore" should {
//    "initialize the database" in {
//      db.init.succeed
//    }
//    "properly handle pagination when merging results from multiple shards" in {
//      val docs = (1 to 300).map(i => TestDoc(value = i)).toList
//
//      db.docs.transaction { implicit transaction =>
//        db.docs.insert(docs).succeed
//      }
//    }
//    "verify the proper counts per shard" in {
//      db.docs.transaction { implicit transaction =>
//        db.docs.asInstanceOf[ShardedStore[TestDoc, TestDoc.type]].shardCounts.map { counts =>
//          counts should be(Vector(
//            100, 100, 100
//          ))
//        }
//      }
//    }
//    "verify all docs were inserted" in {
//      db.docs.transaction { implicit transaction =>
//        db.docs.count.flatMap { count =>
//          count should be(300)
//
//          db.docs.query.sort(Sort.ByField(db.docs.model.value, SortDirection.Ascending)).limit(100).toList.map { results =>
//            results.size should be(100)
//            results.map(_.value) should be((1 to 100).toList)
//          }
//        }
//      }
//    }
//    "properly handle offset when merging results from multiple shards" in {
//      db.docs.transaction { implicit transaction =>
//        db.docs.query.sort(Sort.ByField(db.docs.model.value, SortDirection.Ascending)).offset(100).limit(100).toList.map { results =>
//          results.size should be(100)
//          results.map(_.value) should be((101 to 200).toList)
//        }
//      }
//    }
//    "properly handle sorting when merging results from multiple shards" in {
//      db.docs.transaction { implicit transaction =>
//        db.docs.query.sort(Sort.ByField(db.docs.model.value, SortDirection.Descending)).limit(100).toList.map { results =>
//          results.size should be(100)
//          results.map(_.value) should be((300 to 201 by -1).toList)
//        }
//      }
//    }
//    "truncate the database" in {
//      db.truncate().succeed
//    }
//    "verify the count is now empty" in {
//      db.docs.transaction { implicit transaction =>
//        db.docs.count.map(_ should be(0))
//      }
//    }
//    "dispose the database" in {
//      db.dispose.succeed
//    }
//  }
//
//  case class TestDoc(value: Int, _id: Id[TestDoc] = Id[TestDoc]()) extends Document[TestDoc]
//
//  object TestDoc extends DocumentModel[TestDoc] with JsonConversion[TestDoc] {
//    override implicit val rw: RW[TestDoc] = RW.gen
//
//    val value: Field.Indexed[TestDoc, Int] = field.index("value", (d: TestDoc) => d.value)
//  }
//
//  object db extends LightDB {
//    override type SM = ShardedStoreManager
//    override val storeManager: ShardedStoreManager = ShardedStoreManager(LuceneStore, 3, BalancedShardManager)
//    override lazy val directory: Option[Path] = Some(Path.of(s"db/ShardedStorePaginationSpec"))
//
//    val docs: Collection[TestDoc, TestDoc.type] = store(TestDoc)
//
//    override def upgrades: List[DatabaseUpgrade] = Nil
//  }
//}
