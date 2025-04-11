package lightdb

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.StoreManager

case class MultiStore[Key, Doc <: Document[Doc], Model <: DocumentModel[Doc], SM <: StoreManager](model: Model,
                                                                                                  storeManager: SM,
                                                                                                  nameFromKey: Key => String,
                                                                                                  db: LightDB) {
  private var _stores = Map.empty[Key, storeManager.S[Doc, Model]]

  def stores: Map[Key, storeManager.S[Doc, Model]] = _stores

  def toList: List[storeManager.S[Doc, Model]] = stores.values.toList

  def apply(key: Key): storeManager.S[Doc, Model] = synchronized {
    _stores.get(key) match {
      case Some(store) => store
      case None =>
        val s = db.storeCustom[Doc, Model, SM](model, storeManager, name = Some(nameFromKey(key)))
        _stores += key -> s
        s
    }
  }
}
