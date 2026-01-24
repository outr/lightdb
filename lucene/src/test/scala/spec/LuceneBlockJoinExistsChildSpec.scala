package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, ParentChildSupport, RecordDocument, RecordDocumentModel}
import lightdb.field.Field
import lightdb.filter.FilterExtras
import lightdb.id.Id
import lightdb.lucene.blockjoin.LuceneBlockJoinStore
import lightdb.lucene.blockjoin.LuceneBlockJoinSyntax.*
import lightdb.lucene.LuceneStore
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

/**
 * End-to-end spec for Lucene block-join parent/child queries.
 *
 * This validates:
 * - children indexed first, parent last (block join invariant)
 * - ExistsChild semantics (single, same-child, collective)
 */
@EmbeddedTest
class LuceneBlockJoinExistsChildSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  private val alpha = Parent(name = "Alpha")
  private val bravo = Parent(name = "Bravo")
  private val charlie = Parent(name = "Charlie")
  private val delta = Parent(name = "Delta")
  private val echo = Parent(name = "Echo")

  private val children = List(
    Child(parentId = alpha._id, state = Some("WY")),
    Child(parentId = alpha._id, range = Some("68W")),
    Child(parentId = bravo._id, state = Some("WY"), range = Some("69W")),
    Child(parentId = charlie._id, range = Some("68W")),
    Child(parentId = delta._id, state = Some("UT")),
    Child(parentId = delta._id, range = Some("70W")),
    Child(parentId = echo._id, state = Some("UT"), range = Some("70W"))
  )

  private def byParent(id: Id[Parent]): List[Child] = children.filter(_.parentId == id)

  private lazy val specName: String = getClass.getSimpleName

  specName should {
    "initialize the database" in {
      DB.init.succeed
    }
    "index parent/child blocks" in {
      val parents = List(alpha, bravo, charlie, delta, echo)
      val store = DB.parents
      parents.foldLeft(Task.unit) { case (acc, p) =>
        acc.next(store.indexBlock(p, byParent(p._id)))
      }.next(store.commitIndex()).map(_ => succeed)
    }
    "rebuildFromChildIds produces the same results" in {
      val store = DB.parents
      val parents = List(alpha, bravo, charlie, delta, echo)
      // For this spec, the model's childStore is backed by a LuceneStore (model-only). Populate it so getAll(ids) works.
      DB.childrenStoreForModelOnly.transaction(_.insert(children)).flatMap { _ =>
        store.rebuildFromChildIds(
          parents = rapid.Stream.emits(parents),
          childIdsForParent = p => Task.pure(byParent(p._id).map(_._id)),
          truncateFirst = true,
          commitEvery = 0
        )
      }.flatMap { _ =>
        DB.parents.transaction { tx =>
          tx.query
            .filter(_.childFilter(_.state === Some("WY")))
            .id
            .toList
            .map(_.toSet should be(Set(alpha._id, bravo._id)))
        }
      }
    }
    "match parents when a child satisfies a single condition" in {
      DB.parents.transaction { tx =>
        tx.query
          .filter(_.childFilter(_.state === Some("WY")))
          .id
          .toList
          .map(_.toSet should be(Set(alpha._id, bravo._id)))
      }
    }
    "match parents when different children satisfy different conditions" in {
      DB.parents.transaction { tx =>
        tx.query
          .filter(p => p.childFilter(_.state === Some("WY")) && p.childFilter(_.range === Some("68W")))
          .id
          .toList
          .map(_ should be(List(alpha._id)))
      }
    }
    "match parents when different children satisfy different conditions (collective helper)" in {
      DB.parents.transaction { tx =>
        tx.query
          .filter(_.childFilterCollectiveAll(_.state === Some("WY"), _.range === Some("68W")))
          .id
          .toList
          .map(_ should be(List(alpha._id)))
      }
    }
    "match parents when both conditions are met on a single child" in {
      DB.parents.transaction { tx =>
        tx.query
          .filter(p => p.childFilter(c => c.state === Some("UT") && c.range === Some("70W")))
          .toList
          .map(_.map(_._id).toSet should be(Set(echo._id)))
      }
    }
    "match parents when both conditions are met on a single child (same-child helper)" in {
      DB.parents.transaction { tx =>
        tx.query
          .filter(_.childFilterSameAll(_.state === Some("UT"), _.range === Some("70W")))
          .toList
          .map(_.map(_._id).toSet should be(Set(echo._id)))
      }
    }
    "materialize parent documents (StoreMode.All) from stored parent fields" in {
      DB.parents.transaction { tx =>
        tx.query
          .filter(_.name === "Alpha")
          .toList
          .map { list =>
            list.map(_.name) should be(List("Alpha"))
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

    val parents: LuceneBlockJoinStore[Parent, Child, Child.type, Parent.type] =
      this.blockJoinCollection[Parent, Child, Child.type, Parent.type](
        parentModel = Parent,
        name = Some("entitySearch")
      )
    val childrenStoreForModelOnly: Collection[Child, Child.type] = store(Child) // not used for searching; just provides model+fields

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Parent(name: String,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Parent] = Parent.id()) extends RecordDocument[Parent]

  object Parent extends RecordDocumentModel[Parent] with JsonConversion[Parent] with ParentChildSupport[Parent, Child, Child.type] {
    override implicit val rw: RW[Parent] = RW.gen

    override def childStore: Collection[Child, Child.type] = DB.childrenStoreForModelOnly

    override def parentField(childModel: Child.type): Field[Child, Id[Parent]] = childModel.parentId

    val name: I[String] = field.index(_.name)
  }

  case class Child(parentId: Id[Parent],
                   state: Option[String] = None,
                   range: Option[String] = None,
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Child] = Child.id()) extends RecordDocument[Child]

  object Child extends RecordDocumentModel[Child] with JsonConversion[Child] {
    override implicit val rw: RW[Child] = RW.gen

    val parentId: I[Id[Parent]] = field.index(_.parentId)
    val state: I[Option[String]] = field.index(_.state)
    val range: I[Option[String]] = field.index(_.range)
  }
}


