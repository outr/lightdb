//package lightdb.collection
//
//import fabric.Json
//import fabric.define.DefType
//import fabric.rw._
//import lightdb._
//import lightdb.doc.{Document, DocumentModel, JsonConversion}
//import lightdb.error.{DocNotFoundException, ModelMissingFieldsException}
//import lightdb.field.Field._
//import lightdb.lock.LockManager
//import lightdb.store.split.SplitStore
//import lightdb.store.{Conversion, Store}
//import lightdb.transaction.Transaction
//import lightdb.util.{Disposable, Initializable}
//import rapid._
//import scribe.{rapid => logger}
//
//case class Collection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
//                                                                         model: Model,
//                                                                         store: Store[Doc, Model],
//                                                                         db: LightDB) extends Initializable with Disposable { collection =>
//  /**
//   * Convenience feature for simple one-off operations removing the need to manually create a transaction around it.
//   */
//  object t {
//    def insert(doc: Doc): Task[Doc] = transaction { implicit transaction =>
//      collection.insert(doc)
//    }
//
//    def upsert(doc: Doc): Task[Doc] = transaction { implicit transaction =>
//      collection.upsert(doc)
//    }
//
//    def insert(docs: Seq[Doc]): Task[Seq[Doc]] = transaction { implicit transaction =>
//      collection.insert(docs)
//    }
//
//    def upsert(docs: Seq[Doc]): Task[Seq[Doc]] = transaction { implicit transaction =>
//      collection.upsert(docs)
//    }
//
//    def get[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Option[Doc]] = transaction { implicit transaction =>
//      collection.get(f)
//    }
//
//    def apply[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Doc] = transaction { implicit transaction =>
//      collection(f)
//    }
//
//    def get(id: Id[Doc]): Task[Option[Doc]] = transaction { implicit transaction =>
//      collection.get(id)
//    }
//
//    def getAll(ids: Seq[Id[Doc]]): Task[List[Doc]] = transaction { implicit transaction =>
//      collection.getAll(ids).toList
//    }
//
//    def apply(id: Id[Doc]): Task[Doc] = transaction { implicit transaction =>
//      collection(id)
//    }
//
//    object json {
//      def insert(stream: rapid.Stream[Json],
//                 disableSearchUpdates: Boolean): Task[Int] = transaction { implicit transaction =>
//        if (disableSearchUpdates) {
//          transaction.put(SplitStore.NoSearchUpdates, true)
//        }
//        stream
//          .map(_.as[Doc](model.rw))
//          .evalMap(collection.insert)
//          .count
//      }
//
//      def stream[Return](f: rapid.Stream[Json] => Task[Return]): Task[Return] = transaction { implicit transaction =>
//        f(collection.store.jsonStream)
//      }
//    }
//
//    def list(): Task[List[Doc]] = transaction { implicit transaction =>
//      collection.list()
//    }
//
//    def modify(id: Id[Doc], lock: Boolean = true, deleteOnNone: Boolean = false)
//              (f: Forge[Option[Doc], Option[Doc]]): Task[Option[Doc]] = transaction { implicit transaction =>
//      collection.modify(id, lock, deleteOnNone)(f)
//    }
//
//    def delete[V](f: Model => (UniqueIndex[Doc, V], V)): Task[Boolean] = transaction { implicit transaction =>
//      collection.delete(f)
//    }
//
//    def delete(id: Id[Doc]): Task[Boolean] = transaction { implicit transaction =>
//      collection.delete(id)
//    }
//
//    def count: Task[Int] = transaction { implicit transaction =>
//      collection.count
//    }
//
//    def truncate(): Task[Int] = transaction { implicit transaction =>
//      collection.truncate()
//    }
//  }
//}
//
//object Collection {
//  var CacheQueries: Boolean = false
//  var MaxInsertBatch: Int = 1_000_000
//  var LogTransactions: Boolean = false
//}