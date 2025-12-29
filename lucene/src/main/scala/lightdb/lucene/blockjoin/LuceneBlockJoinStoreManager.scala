package lightdb.lucene.blockjoin

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, ParentChildSupport}
import lightdb.field.Field
import lightdb.store.{CollectionManager, StoreMode}

import java.nio.file.Path

/**
 * Typed CollectionManager for creating a single LuceneBlockJoinStore for a specific Parent/Child relationship.
 *
 * This is intended to be used via `db.storeCustom(...)` so we don't need to infer child types dynamically.
 */
case class LuceneBlockJoinStoreManager[
  Parent <: Document[Parent],
  Child <: Document[Child],
  ChildModel <: DocumentModel[Child],
  ParentModel <: ParentChildSupport[Parent, Child, ChildModel]
](parentFieldFilter: Field[Parent, _] => Boolean = (_: Field[Parent, _]) => true,
  childFieldFilter: Field[Child, _] => Boolean = (f: Field[Child, _]) => f.indexed,
  childStoreAll: Boolean = false) extends CollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = lightdb.store.Collection[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] = {
    val s = new LuceneBlockJoinStore[Parent, Child, ChildModel, ParentModel](
      name = name,
      path = path,
      model = model.asInstanceOf[ParentModel],
      storeMode = storeMode.asInstanceOf[StoreMode[Parent, ParentModel]],
      lightDB = db,
      storeManager = this
    )
    s.parentFieldFilter = parentFieldFilter
    s.childFieldFilter = childFieldFilter
    s.childStoreAll = childStoreAll
    s.asInstanceOf[S[Doc, Model]]
  }
}


