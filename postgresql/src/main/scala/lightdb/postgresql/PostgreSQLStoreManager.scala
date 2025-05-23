package lightdb.postgresql

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.ConnectionManager
import lightdb.store.{StoreManager, StoreMode}

import java.nio.file.Path

case class PostgreSQLStoreManager(connectionManager: ConnectionManager) extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = PostgreSQLStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] =
    new PostgreSQLStore[Doc, Model](name, path, model, connectionManager, storeMode, db, this)
}
