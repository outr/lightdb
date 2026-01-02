package lightdb.lucene.blockjoin

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, ParentChildSupport}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.store.{Collection, CollectionManager, StoreMode}

/**
 * Lucene-specific syntax helpers for defining and using block-join indexes ergonomically.
 */
object LuceneBlockJoinSyntax {
  /**
   * Default store mode used by block-join helpers when none is provided.
   *
   * Kept as an implicit def (instead of default args) because Scala 2.13 can struggle to type-infer
   * default arguments that involve higher-kinded / invariant type parameters.
   */
  implicit def defaultStoreMode[Doc <: Document[Doc], Model <: DocumentModel[Doc]]: StoreMode[Doc, Model] =
    StoreMode.All[Doc, Model]()

  /**
   * A derived Lucene block-join collection optimized for ExistsChild-style queries.
   *
   * This is a third index physically containing both child and parent documents (children first, parent last),
   * so Lucene's block-join queries can run at high speed.
   */
  type JoinedCollection[
    Parent <: Document[Parent],
    Child <: Document[Child],
    ParentModel <: ParentChildSupport[Parent, Child, ChildModel],
    ChildModel <: DocumentModel[Child]
  ] = LuceneBlockJoinDerivedStore[Parent, Child, ChildModel, ParentModel, ParentModel]

  implicit class LuceneBlockJoinAnyLightDBOps(val db: LightDB) extends AnyVal {
    /**
     * Defines a Lucene block-join index as a normal LightDB store (children first, parent last).
     *
     * Populate it using `indexBlock(...)` / `rebuild(...)` / `rebuildFromChildIds(...)`.
     *
     * This API is fully type-safe and intentionally avoids any casting.
     */
    def blockJoinCollection[
      Parent <: Document[Parent],
      Child <: Document[Child],
      ChildModel <: DocumentModel[Child],
      ParentModel <: ParentChildSupport[Parent, Child, ChildModel]
    ](parentModel: ParentModel,
      name: Option[String] = None,
      parentFieldFilter: Field[Parent, _] => Boolean = (_: Field[Parent, _]) => true,
      childFieldFilter: Field[Child, _] => Boolean = (f: Field[Child, _]) => f.indexed,
      childStoreAll: Boolean = false)(implicit storeMode: StoreMode[Parent, ParentModel]): LuceneBlockJoinStore[Parent, Child, ChildModel, ParentModel] = {
      val n = name.getOrElse(parentModel.getClass.getSimpleName.replace("$", ""))
      val path = db.directory.map(_.resolve(n))
      val store = new LuceneBlockJoinStore[Parent, Child, ChildModel, ParentModel](
        name = n,
        path = path,
        model = parentModel,
        storeMode = storeMode,
        lightDB = db,
        storeManager = LuceneStore
      )
      store.parentFieldFilter = parentFieldFilter
      store.childFieldFilter = childFieldFilter
      store.childStoreAll = childStoreAll
      // Scala 2.x inference struggles with higher-kinded bounds here; be explicit and return the concrete store.
      db.registerStore[Parent, ParentModel, lightdb.store.Store[Parent, ParentModel]](store)
      store
    }

  }

  implicit class LuceneBlockJoinCollectionLightDBOps(val db: LightDB { type SM <: CollectionManager }) extends AnyVal {
    /**
     * Defines a derived Lucene block-join index as a normal LightDB collection, sourced from two upstream collections:
     * parent + child.
     *
     * This API is fully type-safe and intentionally avoids any casting.
     */
    def joinedCollection[
      Parent <: Document[Parent],
      Child <: Document[Child],
      ChildModel <: DocumentModel[Child],
      ParentModel <: ParentChildSupport[Parent, Child, ChildModel]
    ](parent: Collection[Parent, ParentModel],
      child: Collection[Child, ChildModel],
      parentId: ChildModel => Field[Child, Id[Parent]],
      name: Option[String] = None,
      parentFieldFilter: Field[Parent, _] => Boolean = (_: Field[Parent, _]) => true,
      childFieldFilter: Field[Child, _] => Boolean = (f: Field[Child, _]) => f.indexed,
      childStoreAll: Boolean = false,
      truncateFirst: Boolean = true,
      commitEvery: Int = 0,
      optimizeAfterRebuild: Boolean = true)(implicit storeMode: StoreMode[Parent, ParentModel]): JoinedCollection[Parent, Child, ParentModel, ChildModel] = {
      // Sanity check: ensure the declared ParentChildSupport wiring matches the explicit join field.
      val expected = parent.model.parentField(child.model)
      val provided = parentId(child.model)
      if (expected.name != provided.name) {
        throw new IllegalArgumentException(
          s"joinedCollection mismatch: parentModel.parentField(childModel) was '${expected.name}' " +
            s"but parentId(childModel) was '${provided.name}'. Ensure these refer to the same field."
        )
      }

      val manager = LuceneBlockJoinDerivedStoreManager[Parent, Child, ChildModel, ParentModel, ParentModel](
        parentSource = () => parent,
        childSource = () => child,
        parentFieldFilter = parentFieldFilter,
        childFieldFilter = childFieldFilter,
        childStoreAll = childStoreAll,
        truncateFirst = truncateFirst,
        commitEvery = commitEvery,
        optimizeAfterRebuild = optimizeAfterRebuild
      )

      val base =
        db.storeCustomWithMode[Parent, ParentModel, LuceneBlockJoinDerivedStoreManager[Parent, Child, ChildModel, ParentModel, ParentModel]](
          model = parent.model,
          storeManager = manager,
          storeMode = storeMode,
          name = name
        )

      base.asInstanceOf[JoinedCollection[Parent, Child, ParentModel, ChildModel]]
    }

  }
}

