package lightdb.qdrant

import io.qdrant.client.{QdrantClient, QdrantGrpcClient}
import io.qdrant.client.grpc.Collections.{Distance, VectorParams}
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.store.{Collection, CollectionManager, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
import lightdb.vector.VectorMetric
import rapid.*

import java.nio.file.Path

/** Connection coordinates for a Qdrant server (gRPC, default port 6334). */
case class QdrantConfig(host: String, port: Int = 6334, useTls: Boolean = false, apiKey: Option[String] = None)

/**
 * `Collection` backed by a Qdrant collection — a vector-first store.
 *
 * Qdrant is purpose-built for vector KNN, which it serves natively (see [[QdrantTransaction]]); it is
 * intentionally NOT a general query engine here. A model's single [[Field.VectorIndex]] becomes the
 * Qdrant point vector; every document field is stored in the point payload (the full document as JSON
 * for exact round-trip, plus indexed scalar fields for filter push-down). Each LightDB document maps
 * to one point whose id is a deterministic UUID derived from the document `_id`.
 *
 * Limitations (by design — Qdrant's strength is vectors, not general storage):
 *  - no prefix scan, so it is not a `PrefixScanningStore` and cannot back the traversal/graph DSL;
 *  - only equality (and simple AND) filters push down to Qdrant; richer predicates and all non-KNN
 *    sorting are evaluated in-memory over a scroll of matching points.
 *
 * Models without a vector field still work as a plain key/value collection (a size-1 placeholder
 * vector is used); KNN is only available when a vector field is declared.
 */
class QdrantStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                     path: Option[Path],
                                                                     model: Model,
                                                                     val storeMode: StoreMode[Doc, Model],
                                                                     config: QdrantConfig,
                                                                     db: LightDB,
                                                                     storeManager: StoreManager) extends Collection[Doc, Model](name, path, model, db, storeManager) {
  override type TX = QdrantTransaction[Doc, Model]

  // The Qdrant gRPC client maintains its own connection; one per store, closed on dispose.
  private lazy val client: QdrantClient = {
    val builder = QdrantGrpcClient.newBuilder(config.host, config.port, config.useTls)
    config.apiKey.foreach(builder.withApiKey)
    new QdrantClient(builder.build())
  }
  private[qdrant] def qdrant: QdrantClient = client

  // Qdrant collection names are safest starting with a letter; prefix anything else (e.g. LightDB's
  // internal `_backingStore`).
  private[qdrant] lazy val collectionName: String =
    if name.headOption.exists(_.isLetter) then name else s"c$name"

  /** The single vector field, if the model declares one. Qdrant points carry one (unnamed) vector. */
  private[qdrant] lazy val vectorField: Option[Field.VectorIndex[Doc]] =
    fields.collectFirst { case vi: Field.VectorIndex[Doc @unchecked] => vi }

  private[qdrant] def vectorSize: Int = vectorField.map(_.dimension).getOrElse(1)

  private[qdrant] def distance: Distance = vectorField.map(_.metric) match {
    case Some(VectorMetric.Cosine) => Distance.Cosine
    case Some(VectorMetric.Euclidean) => Distance.Euclid
    case Some(VectorMetric.DotProduct) => Distance.Dot
    case None => Distance.Euclid // placeholder vector for non-vector models
  }

  // Buffered batching so bulk loads coalesce into a single Qdrant upsert (see applyWriteOps).
  override def defaultBatchConfig: BatchConfig = BatchConfig.Buffered()

  override protected def initialize(): Task[Unit] = super.initialize().next(Task {
    if (!client.collectionExistsAsync(collectionName).get().booleanValue()) {
      val params = VectorParams.newBuilder().setSize(vectorSize.toLong).setDistance(distance).build()
      client.createCollectionAsync(collectionName, params).get()
    }
  })

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] =
    Task(QdrantTransaction(this, parent, writeHandlerFactory))

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task(client.close()))
}

/**
 * Factory for [[QdrantStore]] instances. Each LightDB store maps to one Qdrant collection (named by
 * the store name).
 */
case class QdrantStoreManager(config: QdrantConfig) extends CollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = QdrantStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): QdrantStore[Doc, Model] =
    new QdrantStore[Doc, Model](name, path, model, storeMode, config, db, this)
}
