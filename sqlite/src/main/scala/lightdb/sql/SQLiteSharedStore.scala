package lightdb.sql

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.ConnectionManager
import lightdb.store.{Store, StoreManager, StoreMode}

case class SQLiteSharedStore(connectionManager: ConnectionManager) extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = SQLiteStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] = {
    new SQLiteStore[Doc, Model](
      name = name,
      model = model,
      connectionManager = connectionManager,
      storeMode = storeMode,
      lightDB = db,
      storeManager = this
    )
  }
}
