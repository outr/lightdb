package lightdb.lucene.blockjoin

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, ParentChildSupport}
import lightdb.field.Field
import lightdb.store.{Collection, CollectionManager, StoreMode}

import java.nio.file.Path

/**
 * CollectionManager for creating a LuceneBlockJoinDerivedStore that depends on:
 * - an upstream parent collection (provides the parent stream)
 * - an upstream child collection (used to fetch children per parent)
 *
 * Intended usage:
 * - parent + child are already existing collections (Lucene, SplitCollection, etc.)
 * - create a third, derived Lucene block-join index optimized for ExistsChild queries
 */
case class LuceneBlockJoinDerivedStoreManager[
  Parent <: Document[Parent],
  Child <: Document[Child],
  ChildModel <: DocumentModel[Child],
  ParentModel <: ParentChildSupport[Parent, Child, ChildModel],
  ParentSourceModel <: DocumentModel[Parent]
](
  parentSource: () => Collection[Parent, ParentSourceModel],
  childSource: () => Collection[Child, ChildModel],
  parentFieldFilter: Field[Parent, _] => Boolean = (_: Field[Parent, _]) => true,
  childFieldFilter: Field[Child, _] => Boolean = (f: Field[Child, _]) => f.indexed,
  childStoreAll: Boolean = false,
  truncateFirst: Boolean = true,
  commitEvery: Int = 0,
  optimizeAfterRebuild: Boolean = true
) extends CollectionManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = lightdb.store.Collection[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] = {
    val s = new LuceneBlockJoinDerivedStore[Parent, Child, ChildModel, ParentModel, ParentSourceModel](
      name = name,
      path = path,
      model = model.asInstanceOf[ParentModel],
      storeMode = storeMode.asInstanceOf[StoreMode[Parent, ParentModel]],
      lightDB = db,
      storeManager = this,
      parentSource = parentSource,
      childSource = childSource,
      truncateFirst = truncateFirst,
      commitEvery = commitEvery,
      optimizeAfterRebuild = optimizeAfterRebuild
    )
    s.parentFieldFilter = parentFieldFilter
    s.childFieldFilter = childFieldFilter
    s.childStoreAll = childStoreAll
    s.asInstanceOf[S[Doc, Model]]
  }
}

