package spec

import fabric.*
import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.field.Field
import lightdb.filter.{Condition, Filter, FilterClause, ParentChildRelation}
import lightdb.filter.*
import lightdb.id.Id
import lightdb.store.{Collection, StoreMode}
import lightdb.store.hashmap.HashMapStore
import lightdb.time.Timestamp
import lightdb.transaction.{CollectionTransaction, Transaction}
import lightdb.transaction.batch.BatchConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import rapid.Task

@EmbeddedTest
class OpenSearchJoinBoostDslSpec extends AnyWordSpec with Matchers {
  case class ChildAttr(key: String, percent: Double)
  object ChildAttr {
    implicit val rw: RW[ChildAttr] = RW.gen
  }

  case class Parent(name: String,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Parent] = Parent.id()) extends RecordDocument[Parent]
  object Parent extends RecordDocumentModel[Parent] with JsonConversion[Parent] {
    override implicit val rw: RW[Parent] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  case class Child(parentId: Id[Parent],
                   tag: String,
                   attrs: List[ChildAttr] = Nil,
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Child] = Child.id()) extends RecordDocument[Child]
  object Child extends RecordDocumentModel[Child] with JsonConversion[Child] {
    override implicit val rw: RW[Child] = RW.gen
    trait Attrs extends Nested[List[ChildAttr]] {
      val key: NP[String]
      val percent: NP[Double]
    }
    val parentId: I[Id[Parent]] = field.index(_.parentId)
    val tag: I[String] = field.index(_.tag)
    val attrs: N[Attrs] = field.index.nested[Attrs](_.attrs)
  }

  object DummyDB extends LightDB {
    override type SM = HashMapStore.type
    override val storeManager: HashMapStore.type = HashMapStore
    override def directory = None
    override def upgrades = Nil
  }

  /**
   * Minimal Collection implementation for building ParentChildRelation without running any transactions.
   */
  class DummyCollection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String, model: Model)
    extends Collection[Doc, Model](name, path = None, model = model, lightDB = DummyDB, storeManager = HashMapStore) {
    override type TX = CollectionTransaction[Doc, Model]
    override def storeMode: StoreMode[Doc, Model] = StoreMode.All()
    override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                             batchConfig: BatchConfig,
                                             writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] =
      Task.error(new UnsupportedOperationException("DummyCollection does not support transactions"))
  }

  "OpenSearchSearchBuilder join boost" should {
    "apply boosts directly on has_child (avoid function_score wrapping)" in {
      val childStore: Collection[Child, Child.type] = new DummyCollection[Child, Child.type]("Children", Child)
      val relation = ParentChildRelation[Parent, Child, Child.type](childStore, _ => Child.parentId)

      val existsChild = Filter.ExistsChild(relation, (_: Child.type) => Filter.Equals[Child, String](Child.tag, "a"))
      val filter = Filter.Multi[Parent](minShould = 0, filters = List(
        FilterClause(filter = existsChild, condition = Condition.Must, boost = Some(2.0))
      ))

      val builder = new lightdb.opensearch.query.OpenSearchSearchBuilder[Parent, Parent.type](Parent, joinScoreMode = "none")
      val dsl = builder.filterToDsl(filter)

      val must0 = dsl.asObj.value("bool").asObj.value("must").asArr.value.head
      val hasChildObj = must0.asObj.value("has_child").asObj

      hasChildObj.value("boost") shouldBe num(2.0)
    }

    "compile nested filters under has_child with same-element semantics" in {
      val childStore: Collection[Child, Child.type] = new DummyCollection[Child, Child.type]("Children", Child)
      val relation = ParentChildRelation[Parent, Child, Child.type](childStore, _ => Child.parentId)

      val nestedChildFilter = Child.attrs.nested { attrs =>
        attrs.key === "tract-a" && attrs.percent >= 0.5
      }
      val existsChild = Filter.ExistsChild(relation, (_: Child.type) => nestedChildFilter)
      val builder = new lightdb.opensearch.query.OpenSearchSearchBuilder[Parent, Parent.type](Parent, joinScoreMode = "none")
      val dsl = builder.filterToDsl(existsChild)

      val hasChildObj = dsl.asObj.value("has_child").asObj
      val nestedObj = hasChildObj.value("query").asObj.value("nested").asObj
      nestedObj.value("path") shouldBe str("attrs")

      val must = nestedObj.value("query").asObj.value("bool").asObj.value("must").asArr.value
      must.exists(_.asObj.value.contains("range")) shouldBe true
      must.exists { clause =>
        val keys = clause.asObj.value.keySet
        keys.contains("bool") || keys.contains("term")
      } shouldBe true
    }

    "fail fast when nested path is not declared on the model" in {
      val childStore: Collection[Child, Child.type] = new DummyCollection[Child, Child.type]("Children", Child)
      val relation = ParentChildRelation[Parent, Child, Child.type](childStore, _ => Child.parentId)

      val invalid = Filter.ExistsChild(relation, (_: Child.type) => Filter.Nested[Child]("unknown", Filter.Equals[Child, String]("key", "x")))
      val builder = new lightdb.opensearch.query.OpenSearchSearchBuilder[Parent, Parent.type](Parent, joinScoreMode = "none")

      val t = intercept[IllegalArgumentException] {
        builder.filterToDsl(invalid)
      }
      t.getMessage should include ("Nested path 'unknown' is not declared")
    }
  }
}


