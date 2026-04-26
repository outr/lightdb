package lightdb.tantivy

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Collection, CollectionManager, NestedQueryCapability, StoreManager, StoreMode}
import lightdb.transaction.{Transaction, WriteHandler}
import lightdb.transaction.batch.BatchConfig
import rapid.*
import scantivy.{Tantivy, TantivyIndex}
import scantivy.proto as pb

import java.nio.file.{Files, Path}

class TantivyStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  name: String,
  path: Option[Path],
  model: Model,
  val storeMode: StoreMode[Doc, Model],
  lightDB: LightDB,
  storeManager: StoreManager
) extends Collection[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX = TantivyTransaction[Doc, Model]

  // Tantivy lacks native block-join, but LightDB's `Fallback` capability lets us run a broad
  // (non-nested) query and post-filter the results in-memory via `NestedQuerySupport.eval`.
  override def nestedQueryCapability: NestedQueryCapability = NestedQueryCapability.Fallback
  override def supportsNativeExistsChild: Boolean = false
  override def defaultBatchConfig: BatchConfig = BatchConfig.Direct

  private[tantivy] lazy val schemaBuilt: TantivySchema.Built = TantivySchema.build(model, storeMode)
  private[tantivy] lazy val tantivyIndex: TantivyIndex = {
    path.foreach(p => Files.createDirectories(p))
    val req = pb.CreateIndexRequest(
      path = path.map(_.toAbsolutePath.toString),
      schema = Some(schemaBuilt.schema),
      idField = Some(schemaBuilt.idField)
    )
    Tantivy.create(req).fold(
      e => throw new RuntimeException(s"Failed to create Tantivy index for store '$name': $e"),
      identity
    )
  }

  override protected def initialize(): Task[Unit] = super.initialize().next(Task {
    // Force lazy init so backing index exists before first use.
    val _ = tantivyIndex
  })

  override def optimize(): Task[Unit] = Task {
    tantivyIndex.optimize(1).fold(e => throw new RuntimeException(e), _ => ())
  }

  override protected def createTransaction(
    parent: Option[Transaction[Doc, Model]],
    batchConfig: BatchConfig,
    writeHandlerFactory: Transaction[Doc, Model] => WriteHandler[Doc, Model]
  ): Task[TX] = Task(TantivyTransaction(this, parent, writeHandlerFactory))

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task {
    tantivyIndex.close()
  })
}

object TantivyStore extends CollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = TantivyStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    db: LightDB,
    model: Model,
    name: String,
    path: Option[Path],
    storeMode: StoreMode[Doc, Model]
  ): S[Doc, Model] = new TantivyStore[Doc, Model](name, path, model, storeMode, db, this)
}
