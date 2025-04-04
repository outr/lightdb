package lightdb.sql

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.ConnectionManager
import lightdb.store.{CollectionManager, Store, StoreManager, StoreMode}

import java.nio.file.Path

case class SQLiteSharedStore(connectionManager: ConnectionManager) extends CollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = SQLiteStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] = {
    new SQLiteStore[Doc, Model](
      name = name,
      path = path,
      model = model,
      connectionManager = connectionManager,
      storeMode = storeMode,
      lightDB = db,
      storeManager = this
    )
  }
}
