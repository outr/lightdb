package lightdb.arangodb

import com.arangodb.{ArangoCollection, ArangoDB, ArangoDBException, ArangoDatabase}
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Collection, CollectionManager, NestedQueryCapability, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
import rapid.*

import java.nio.file.Path

/** Connection coordinates for an ArangoDB server. */
case class ArangoDBConfig(host: String, port: Int, user: String, password: String)

/**
 * `Collection` backed by an ArangoDB document collection (via the ArangoDB Java driver).
 *
 * LightDB's `_id` maps to ArangoDB's `_key` (its reserved `_id`/`_rev` system attributes are stripped
 * on read). Each LightDB instance gets its own ArangoDB database; each store its own collection.
 * Queries are translated to AQL; nested-document predicates resolve natively via AQL array filters.
 * Spatial filters, distance sort, and hierarchical drill-down facets are evaluated in-memory.
 */
class ArangoDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                       path: Option[Path],
                                                                       model: Model,
                                                                       val storeMode: StoreMode[Doc, Model],
                                                                       config: ArangoDBConfig,
                                                                       databaseName: String,
                                                                       db: LightDB,
                                                                       storeManager: StoreManager) extends Collection[Doc, Model](name, path, model, db, storeManager) {
  override type TX = ArangoDBTransaction[Doc, Model]

  private lazy val client: ArangoDB =
    new ArangoDB.Builder().host(config.host, config.port).user(config.user).password(config.password).build()

  private[arangodb] lazy val database: ArangoDatabase = {
    val d = client.db(databaseName)
    try if (!d.exists()) d.create() catch { case _: ArangoDBException => () } // tolerate concurrent create
    d
  }

  // ArangoDB reserves leading-underscore names for system collections (e.g. LightDB's internal
  // `_backingStore`), so prefix any name that doesn't start with a letter.
  private[arangodb] lazy val arangoName: String =
    if name.headOption.exists(_.isLetter) then name else s"c$name"

  private[arangodb] lazy val collection: ArangoCollection = {
    val c = database.collection(arangoName)
    try if (!c.exists()) c.create() catch { case _: ArangoDBException => () }
    c
  }

  // ArangoDB resolves nested-document predicates natively via AQL array filters (== SameElementAll).
  override def nestedQueryCapability: NestedQueryCapability = NestedQueryCapability.Native

  // Buffered batching so bulk loads coalesce into ArangoDB bulk document operations rather than one
  // HTTP round-trip per document (see ArangoDBTransaction.applyWriteOps).
  override def defaultBatchConfig: BatchConfig = BatchConfig.Buffered()

  override protected def initialize(): Task[Unit] = super.initialize().next(Task(collection).unit)

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] =
    Task(ArangoDBTransaction(this, parent, writeHandlerFactory))

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task(client.shutdown()))
}

/**
 * Factory for [[ArangoDBStore]] instances.
 *
 * @param databaseName the ArangoDB database to use; defaults to the LightDB instance name when `None`
 */
case class ArangoDBStoreManager(config: ArangoDBConfig,
                                databaseName: Option[String] = None) extends CollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = ArangoDBStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): ArangoDBStore[Doc, Model] =
    new ArangoDBStore[Doc, Model](name, path, model, storeMode, config, databaseName.getOrElse(db.name), db, this)
}
