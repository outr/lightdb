package spec

import fabric.rw._
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion, ParentChildSupport}
import lightdb.filter.Filter
import lightdb.id.Id
import lightdb.store.{Collection, StoreManager, StoreMode}
import lightdb.transaction.{CollectionTransaction, Transaction}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import rapid.Task

import java.nio.file.Path

@EmbeddedTest
class ParentChildSupportSemanticsSpec extends AnyWordSpec with Matchers {
  case class Parent(_id: Id[Parent] = Parent.id()) extends Document[Parent]
  case class Child(parentId: Id[Parent], value: String, _id: Id[Child] = Child.id()) extends Document[Child]

  object Child extends DocumentModel[Child] with JsonConversion[Child] {
    override implicit val rw: RW[Child] = RW.gen
    val parentId = field.index(_.parentId)
    val value = field.index(_.value)
  }

  object Parent extends DocumentModel[Parent] with JsonConversion[Parent] with ParentChildSupport[Parent, Child, Child.type] {
    override implicit val rw: RW[Parent] = RW.gen
    override def childStore: Collection[Child, Child.type] = DB.children
    override def parentField(childModel: Child.type) = childModel.parentId
  }

  private class DummyTx[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    override val store: Collection[Doc, Model],
    override val parent: Option[Transaction[Doc, Model]]
  ) extends CollectionTransaction[Doc, Model] {
    override def doSearch[V](query: lightdb.Query[Doc, Model, V]) = ???
    override def aggregate(query: lightdb.aggregate.AggregateQuery[Doc, Model]) = ???
    override def aggregateCount(query: lightdb.aggregate.AggregateQuery[Doc, Model]) = ???

    override def jsonStream = ???
    override def truncate = ???

    override protected def _get[V](index: lightdb.field.Field.UniqueIndex[Doc, V], value: V) = ???
    override protected def _insert(doc: Doc) = ???
    override protected def _upsert(doc: Doc) = ???
    override protected def _exists(id: Id[Doc]) = ???
    override protected def _count = ???
    override protected def _delete[V](index: lightdb.field.Field.UniqueIndex[Doc, V], value: V) = ???
    override protected def _commit = ???
    override protected def _rollback = ???
    override protected def _close = ???
  }

  private class DummyCollection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    name: String,
    path: Option[Path],
    model: Model,
    override val storeMode: StoreMode[Doc, Model],
    db: LightDB,
    storeManager: StoreManager
  ) extends Collection[Doc, Model](name, path, model, db, storeManager) {
    override type TX = DummyTx[Doc, Model]
    override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] =
      Task.error(new RuntimeException("DummyCollection transactions are not supported in ParentChildSupportSemanticsSpec"))
  }

  private object DummyStore extends StoreManager {
    override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = DummyCollection[Doc, Model]
    override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
      db: LightDB,
      model: Model,
      name: String,
      path: Option[Path],
      storeMode: StoreMode[Doc, Model]
    ): DummyCollection[Doc, Model] = new DummyCollection[Doc, Model](name, path, model, storeMode, db, this)
  }

  private object DB extends LightDB {
    override type SM = DummyStore.type
    override val storeManager: DummyStore.type = DummyStore
    override def directory: Option[Path] = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    lazy val parents: DummyCollection[Parent, Parent.type] = store(Parent)
    lazy val children: DummyCollection[Child, Child.type] = store(Child)
  }

  "ParentChildSupport helper APIs" should {
    "build same-child semantics as a single ExistsChild whose child filter ANDs criteria" in {
      val f = Parent.childFilterSameAll(
        cm => Filter.Equals(cm.value, "c1"),
        cm => Filter.Equals(cm.parentId, Id[Parent]("p1"))
      )

      val cc = f match {
        case c: Filter.ChildConstraints[Parent] => c
        case other => fail(s"Expected ChildConstraints, got: $other")
      }

      cc.semantics shouldBe Filter.ChildSemantics.SameChildAll
      cc.builds.length shouldBe 2
      cc.expandToExistsChildFilters shouldBe a[Filter.ExistsChild[_]]
    }

    "build collective semantics as AND of multiple ExistsChild filters" in {
      val f = Parent.childFilterCollectiveAll(
        cm => Filter.Equals(cm.value, "c1"),
        cm => Filter.Equals(cm.parentId, Id[Parent]("p1"))
      )

      val cc = f match {
        case c: Filter.ChildConstraints[Parent] => c
        case other => fail(s"Expected ChildConstraints, got: $other")
      }

      cc.semantics shouldBe Filter.ChildSemantics.CollectiveAll
      cc.builds.length shouldBe 2
      cc.expandToExistsChildFilters shouldBe a[Filter.Multi[_]]
    }

    "treat empty same-child semantics as 'exists any child'" in {
      val f = Parent.childFilterSameAll()
      val cc = f.asInstanceOf[Filter.ChildConstraints[Parent]]
      cc.semantics shouldBe Filter.ChildSemantics.SameChildAll
      cc.builds shouldBe Nil
      cc.expandToExistsChildFilters shouldBe a[Filter.ExistsChild[_]]
    }

    "treat empty collective semantics as a no-op filter" in {
      val f = Parent.childFilterCollectiveAll()
      val cc = f.asInstanceOf[Filter.ChildConstraints[Parent]]
      cc.semantics shouldBe Filter.ChildSemantics.CollectiveAll
      cc.builds shouldBe Nil
      val expanded = cc.expandToExistsChildFilters.asInstanceOf[Filter.Multi[Parent]]
      expanded.minShould shouldBe 0
      expanded.filters shouldBe Nil
    }
  }
}


