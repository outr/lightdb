package lightdb.lucene

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{CollectionManager, StoreMode}

import java.nio.file.Path

/** Creates Lucene stores with an explicit [[LuceneDurability]]; see [[LuceneStore.withDurability]]. */
case class LuceneStoreManager(durability: LuceneDurability) extends CollectionManager {
  override lazy val name: String = s"LuceneStore($durability)"

  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = LuceneStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] =
    new LuceneStore[Doc, Model](name, path, model, storeMode, db, this, durability)
}
