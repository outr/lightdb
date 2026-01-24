package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, ParentChildSupport, RecordDocument, RecordDocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.lucene.blockjoin.LuceneBlockJoinSyntax.*
import lightdb.store.{Collection, CollectionManager, StoreMode}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

/**
 * Demonstrates defining a third "joined" Lucene block-join index as a normal LightDB collection,
 * but sourced from two other existing collections (parent + child indexes).
 */
@EmbeddedTest
class LuceneBlockJoinDerivedIndexSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  private lazy val specName: String = getClass.getSimpleName

  private val alpha = Parent(name = "Alpha")
  private val bravo = Parent(name = "Bravo")
  private val children = List(
    Child(parentId = alpha._id, state = Some("WY")),
    Child(parentId = bravo._id, state = Some("UT")),
    Child(parentId = bravo._id, state = Some("WY"))
  )

  specName should {
    "initialize the database" in {
      DB.init.succeed
    }
    "populate the upstream parent and child indexes" in {
      DB.parentsIndex.transaction(_.insert(List(alpha, bravo)))
        .flatMap(_ => DB.childrenIndex.transaction(_.insert(children)))
        .map(_ => succeed)
    }
    "build the derived block-join index from the two upstream indexes" in {
      DB.joined.reIndex().map(_ => succeed)
    }
    "execute ExistsChild queries against the derived block-join index" in {
      DB.joined.transaction { tx =>
        for
          wy <- tx.query.filter(_.childFilter(_.state === Some("WY"))).id.toList
          ut <- tx.query.filter(_.childFilter(_.state === Some("UT"))).id.toList
        yield {
          wy.toSet shouldBe Set(alpha._id, bravo._id)
          ut.toSet shouldBe Set(bravo._id)
        }
      }
    }
    "truncate and dispose the database" in {
      DB.truncate().flatMap(_ => DB.dispose).succeed
    }
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = LuceneStore

    override def name: String = specName
    override lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    // Two existing indexes:
    lazy val parentsIndex: Collection[Parent, Parent.type] = storeCustom(Parent, LuceneStore, name = Some("parentsIndex"))
    lazy val childrenIndex: Collection[Child, Child.type] = storeCustom(Child, LuceneStore, name = Some("childrenIndex"))

    // The third joined index:
    lazy val joined: JoinedCollection[Parent, Child, Parent.type, Child.type] = this.joinedCollection(
      parent = parentsIndex,
      child = childrenIndex,
      parentId = _.parentId,
      name = Some("joinedIndex"),
      parentFieldFilter = f => f.name == "_id" || f.name == "name",
      childFieldFilter = f => f.indexed,
      optimizeAfterRebuild = false
    )

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

    // Wire ParentChildSupport to the existing child index dependency.
    override def childStore: Collection[Child, Child.type] = DB.childrenIndex

    override def parentField(childModel: Child.type): Field[Child, Id[Parent]] = childModel.parentId

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

