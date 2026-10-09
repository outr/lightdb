package lightdb.postgresql

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.ConnectionManager
import lightdb.sql.SQLCollectionManager
import lightdb.store.StoreMode

import java.nio.file.Path

case class PostgreSQLStoreManager(connectionManager: ConnectionManager) extends SQLCollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = PostgreSQLStore[Doc, Model]

  /** A transaction on one of its stores can join another on the same connection manager: it borrows the owner's
    * connection, and the owner's COMMIT or rollback decides both. */
  override def supportsJoin: Boolean = true

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] =
    new PostgreSQLStore[Doc, Model](name, path, model, connectionManager, storeMode, db, this)
}
