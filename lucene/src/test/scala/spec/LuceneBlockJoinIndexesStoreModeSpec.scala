package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, ParentChildSupport, RecordDocument, RecordDocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.lucene.blockjoin.LuceneBlockJoinStore
import lightdb.lucene.blockjoin.LuceneBlockJoinSyntax.*
import lightdb.store.{Collection, StoreManager, StoreMode}
import lightdb.store.hashmap.HashMapStore
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

/**
 * Validates that Lucene (including block-join) can run in StoreMode.Indexes(storage) and materialize
 * full parent documents via the storage transaction, even when indexed fields are NOT stored in Lucene.
 */
@EmbeddedTest
class LuceneBlockJoinIndexesStoreModeSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  private lazy val specName: String = getClass.getSimpleName

  private val alpha = Parent(name = "Alpha")
  private val bravo = Parent(name = "Bravo")
  private val children = List(
    Child(parentId = alpha._id, state = Some("WY")),
    Child(parentId = bravo._id, state = Some("UT"))
  )

  private def byParent(id: Id[Parent]): List[Child] = children.filter(_.parentId == id)

  specName should {
    "initialize the database" in {
      DB.init.succeed
    }
    "insert parent docs into storage" in {
      DB.parentsStorage.transaction(_.insert(List(alpha, bravo))).succeed
    }
    "build the block-join search index (no parent stored fields except _id)" in {
      val store = DB.entitySearch
      List(alpha, bravo).foldLeft(Task.unit) { case (acc, p) =>
        acc.next(store.indexBlock(p, byParent(p._id)))
      }.next(store.commitIndex()).succeed
    }
    "query the search index and materialize full parents from storage" in {
      DB.entitySearch.transaction { tx =>
        tx.query
          .filter(_.name === "Alpha")
          .toList
          .map { list =>
            list.map(_.name) should be(List("Alpha"))
          }
      }
    }
    "query by child filter and materialize full parents from storage" in {
      DB.entitySearch.transaction { tx =>
        tx.query
          .filter(_.childFilter(_.state === Some("UT")))
          .toList
          .map { list =>
            list.map(_.name) should be(List("Bravo"))
          }
      }
    }
    "truncate and dispose the database" in {
      DB.truncate().flatMap(_ => DB.dispose).succeed
    }
  }

  object DB extends LightDB {
    override type SM = StoreManager
    override val storeManager: StoreManager = HashMapStore

    override def name: String = specName
    override lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val parentsStorage: HashMapStore[Parent, Parent.type] =
      store[Parent, Parent.type](Parent).asInstanceOf[HashMapStore[Parent, Parent.type]]

    // Child store is model-only (ParentChildSupport requires a Collection). We don't use it for searching here.
    val childrenStoreForModelOnly: Collection[Child, Child.type] =
      storeCustom[Child, Child.type, lightdb.store.CollectionManager](Child, LuceneStore, name = Some("childrenModelOnly"))

    val entitySearch: LuceneBlockJoinStore[Parent, Child, Child.type, Parent.type] =
      this.blockJoinCollection[Parent, Child, Child.type, Parent.type](
        parentModel = Parent,
        name = Some("entitySearch"),
        parentFieldFilter = (f: Field[Parent, _]) => f.name == "_id" || f.name == "name", // keep it tiny; name is indexed but not stored
        childFieldFilter = (f: Field[Child, _]) => f.indexed,
        childStoreAll = false
      )(StoreMode.Indexes(parentsStorage))

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Parent(name: String,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Parent] = Parent.id()) extends RecordDocument[Parent]

  object Parent extends RecordDocumentModel[Parent]
    with JsonConversion[Parent]
    with ParentChildSupport[Parent, Child, Child.type] {
    override implicit val rw: RW[Parent] = RW.gen

    override def childStore: Collection[Child, Child.type] = DB.childrenStoreForModelOnly
    override def parentField(childModel: Child.type): Field[Child, Id[Parent]] = childModel.parentId

    // Critical for this spec: index name but do NOT store it in Lucene.
    val name: I[String] = field.index(_.name, stored = false)
  }

  case class Child(parentId: Id[Parent],
                   state: Option[String] = None,
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Child] = Child.id()) extends RecordDocument[Child]

  object Child extends RecordDocumentModel[Child] with JsonConversion[Child] {
    override implicit val rw: RW[Child] = RW.gen

    val parentId: I[Id[Parent]] = field.index(_.parentId)
    val state: I[Option[String]] = field.index(_.state)
  }
}


