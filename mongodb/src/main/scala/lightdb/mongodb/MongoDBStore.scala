package lightdb.mongodb

import com.mongodb.client.{MongoClient, MongoClients, MongoCollection, MongoDatabase}
import com.mongodb.client.model.Indexes
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Collection, CollectionManager, NestedQueryCapability, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
import org.bson.Document as BsonDoc
import rapid.*

import java.nio.file.Path

/**
 * `Collection` backed by a MongoDB collection.
 *
 * Phase 1 delivered the KV contract (`_id`-based CRUD, streaming, count, truncate) with native
 * strict-insert via the `_id` primary key. Phase 2 adds the query surface: filtering, sorting,
 * pagination, count/countTotal, and all `Conversion` projections. Native nested (`$elemMatch`) and
 * aggregation pipelines are layered on in Phase 3.
 *
 * Each LightDB store maps to one MongoDB collection (named by the store name) inside `databaseName`
 * (defaults to the LightDB instance name).
 */
class MongoDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                      path: Option[Path],
                                                                      model: Model,
                                                                      val storeMode: StoreMode[Doc, Model],
                                                                      connectionString: String,
                                                                      databaseName: String,
                                                                      db: LightDB,
                                                                      storeManager: StoreManager) extends Collection[Doc, Model](name, path, model, db, storeManager) {
  override type TX = MongoDBTransaction[Doc, Model]

  // MongoClient is thread-safe and maintains its own connection pool. One client per store keeps the
  // lifecycle simple (closed on dispose); sharing a single client across a database's stores is a
  // possible later optimization.
  private lazy val client: MongoClient = MongoClients.create(connectionString)
  private lazy val database: MongoDatabase = client.getDatabase(databaseName)
  private[mongodb] lazy val collection: MongoCollection[BsonDoc] =
    database.getCollection(name, classOf[BsonDoc])

  // Buffered batching so bulk loads coalesce into MongoDB `bulkWrite` calls
  // (see MongoDBTransaction.applyWriteOps) rather than one network round-trip per document.
  override def defaultBatchConfig: BatchConfig = BatchConfig.Buffered()

  // MongoDB resolves nested-document predicates natively via `$elemMatch` (== SameElementAll).
  override def nestedQueryCapability: NestedQueryCapability = NestedQueryCapability.Native

  override protected def initialize(): Task[Unit] = super.initialize().next(Task {
    // Best-effort secondary indexes for indexed fields. `_id` is always indexed by MongoDB; spatial
    // and whole-document projections are skipped. Failures (e.g. unindexable shapes) are non-fatal.
    fields.filter(f => f.indexed && f.name != "_id" && !f.isSpatial).foreach { f =>
      try collection.createIndex(Indexes.ascending(f.name))
      catch { case t: Throwable => scribe.debug(s"Skipping MongoDB index on $name.${f.name}: ${t.getMessage}") }
    }
  })

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] =
    Task(MongoDBTransaction(this, parent, writeHandlerFactory))

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task(client.close()))
}

/**
 * Factory for [[MongoDBStore]] instances.
 *
 * @param connectionString a MongoDB connection URI (e.g. `mongodb://localhost:27017`)
 * @param databaseName     the database to use; defaults to the LightDB instance name when `None`
 */
case class MongoDBStoreManager(connectionString: String,
                               databaseName: Option[String] = None) extends CollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = MongoDBStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): MongoDBStore[Doc, Model] =
    new MongoDBStore[Doc, Model](name, path, model, storeMode, connectionString, databaseName.getOrElse(db.name), db, this)
}
