package lightdb.mariadb

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.SQLCollectionManager
import lightdb.sql.connect.ConnectionManager
import lightdb.store.StoreMode

import java.nio.file.Path

case class MariaDBStoreManager(connectionManager: ConnectionManager) extends SQLCollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = MariaDBStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] =
    new MariaDBStore[Doc, Model](name, path, model, connectionManager, storeMode, db, this)
}
