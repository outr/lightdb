package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.filter.Filter
import lightdb.id.Id
import lightdb.store.{Collection, StoreManager, StoreMode}
import lightdb.time.Timestamp
import lightdb.transaction.{CollectionTransaction, Transaction}
import lightdb.transaction.batch.BatchConfig
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import rapid.Task

import java.nio.file.Path

@EmbeddedTest
class NestedUnsupportedStoreSpec extends AnyWordSpec with Matchers {
  case class Attr(key: String, percent: Double)
  object Attr {
    implicit val rw: RW[Attr] = RW.gen
  }

  case class Entry(attrs: List[Attr],
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Entry] = Entry.id()) extends Document[Entry]
  object Entry extends DocumentModel[Entry] with JsonConversion[Entry] {
    override implicit val rw: RW[Entry] = RW.gen
    trait Attrs extends Nested[List[Attr]] {
      val key: NP[String]
      val percent: NP[Double]
    }
    val attrs: N[Attrs] = field.index.nested[Attrs](_.attrs)
  }

  private class DummyTx[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    override val store: Collection[Doc, Model],
    override val parent: Option[Transaction[Doc, Model]],
    writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]
  ) extends CollectionTransaction[Doc, Model] {
    override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)
    override def doSearch[V](query: lightdb.Query[Doc, Model, V]) = Task.error(new RuntimeException("doSearch should not be called"))
    override def aggregate(query: lightdb.aggregate.AggregateQuery[Doc, Model]) = ???
    override def aggregateCount(query: lightdb.aggregate.AggregateQuery[Doc, Model]) = ???
    override def jsonStream = ???
    override def truncate = ???
    override protected def _get[V](index: lightdb.field.Field.UniqueIndex[Doc, V], value: V) = ???
    override protected def _insert(doc: Doc) = ???
    override protected def _upsert(doc: Doc) = ???
    override protected def _exists(id: Id[Doc]) = ???
    override protected def _count = ???
    override protected def _delete(id: Id[Doc]) = ???
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
    override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                             batchConfig: BatchConfig,
                                             writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] =
      Task.error(new RuntimeException("DummyCollection transactions are not supported in NestedUnsupportedStoreSpec"))
  }

  private object DummyStore extends StoreManager {
    override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = DummyCollection[Doc, Model]
    override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                            model: Model,
                                                                            name: String,
                                                                            path: Option[Path],
                                                                            storeMode: StoreMode[Doc, Model]): DummyCollection[Doc, Model] =
      new DummyCollection[Doc, Model](name, path, model, storeMode, db, this)
  }

  private object DB extends LightDB {
    override type SM = DummyStore.type
    override val storeManager: DummyStore.type = DummyStore
    override def directory: Option[Path] = None
    override def upgrades: List[DatabaseUpgrade] = Nil
    lazy val entries: DummyCollection[Entry, Entry.type] = store(Entry)
  }

  "Nested queries on unsupported stores" should {
    "fail fast with a clear unsupported error" in {
      val tx = new DummyTx[Entry, Entry.type](DB.entries, None, _ => null)
      val q = tx.query.filter(_.nested("attrs")(_ => Filter.Exact[Entry, String]("key", "tract-a")))
      val failure = q.search.attempt.sync().failed.get
      failure shouldBe a[UnsupportedOperationException]
      failure.getMessage.toLowerCase should include("nested queries are not supported")
    }
  }
}

