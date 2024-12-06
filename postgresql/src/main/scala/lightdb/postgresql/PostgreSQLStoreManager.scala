package lightdb.postgresql

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.ConnectionManager
import lightdb.store.{Store, StoreManager, StoreMode}

case class PostgreSQLStoreManager(connectionManager: ConnectionManager, connectionShared: Boolean) extends StoreManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] =
    new PostgreSQLStore[Doc, Model](name, model, connectionManager, connectionShared, storeMode)
}
