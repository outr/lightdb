package lightdb.postgresql

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.ConnectionManager
import lightdb.store.{Store, StoreManager, StoreMode}

case class PostgreSQLStoreManager(connectionManager: ConnectionManager) extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = PostgreSQLStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] =
    new PostgreSQLStore[Doc, Model](name, model, connectionManager, storeMode, db, this)
}
