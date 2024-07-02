package lightdb.store

import lightdb.LightDB
import lightdb.doc.DocModel

trait StoreManager {
  def create[Doc, Model <: DocModel[Doc]](db: LightDB, name: String): Store[Doc, Model]
}
