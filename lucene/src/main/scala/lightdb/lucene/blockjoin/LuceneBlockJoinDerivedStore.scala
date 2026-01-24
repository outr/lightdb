package lightdb.lucene.blockjoin

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, ParentChildSupport}
import lightdb.filter.Filter
import lightdb.progress.ProgressManager
import lightdb.store.{Collection, CollectionManager, StoreMode}
import org.apache.lucene.index.Term
import org.apache.lucene.search.TermQuery
import rapid.{Stream, Task}

import java.nio.file.Path

/**
 * A LuceneBlockJoinStore that can (re)build itself from two upstream collections:
 * - a parent source collection (provides the stream of Parent docs)
 * - a child source collection (used to fetch children for each parent)
 *
 * This is intended for "derived search index" use-cases where Parent and Child already exist (often as Lucene stores,
 * SplitCollections, etc.) and we want a third index optimized for ExistsChild-style queries.
 *
 * Notes:
 * - Parent/child join semantics still require block indexing (children first, parent last).
 * - Rebuild is currently sequential (consistent with LuceneBlockJoinStore.rebuild).
 */
class LuceneBlockJoinDerivedStore[
  Parent <: Document[Parent],
  Child <: Document[Child],
  ChildModel <: DocumentModel[Child],
  ParentModel <: ParentChildSupport[Parent, Child, ChildModel],
  ParentSourceModel <: DocumentModel[Parent]
](name: String,
  path: Option[Path],
  model: ParentModel,
  override val storeMode: StoreMode[Parent, ParentModel],
  lightDB: LightDB,
  storeManager: CollectionManager,
  parentSource: () => Collection[Parent, ParentSourceModel],
  childSource: () => Collection[Child, ChildModel],
  truncateFirst: Boolean,
  commitEvery: Int,
  optimizeAfterRebuild: Boolean) extends LuceneBlockJoinStore[Parent, Child, ChildModel, ParentModel](
  name = name,
  path = path,
  model = model,
  storeMode = storeMode,
  lightDB = lightDB,
  storeManager = storeManager
) {
  private def parentTypeTermQuery: TermQuery =
    new TermQuery(new Term(LuceneBlockJoinFields.TypeField, LuceneBlockJoinFields.ParentTypeValue))

  /**
   * Counts only parent documents in this block-join index (children are excluded).
   */
  private def countIndexedParents: Task[Int] = Task {
    val s = index.createIndexSearcher()
    try {
      s.count(parentTypeTermQuery)
    } finally {
      index.releaseIndexSearch(s)
    }
  }

  /**
   * Counts parent documents in the upstream parent source.
   */
  private def countSourceParents(progressManager: ProgressManager): Task[Int] =
    parentSource().transaction { tx =>
      tx.count
    }

  override def verify(progressManager: ProgressManager = ProgressManager.none): Task[Boolean] = for
    src <- countSourceParents(progressManager)
    idx <- countIndexedParents
    outOfSync = src != idx
    _ <- reIndex(progressManager).when(outOfSync)
  yield outOfSync

  /**
   * Rebuilds this joined index from the upstream collections.
   *
   * Join rule: child documents are linked to a parent using ParentChildSupport.parentField(childModel) == parent._id.
   */
  override def reIndex(progressManager: ProgressManager = ProgressManager.none): Task[Boolean] = {
    val ps = parentSource()
    val cs = childSource()
    val joinField = model.parentField(cs.model)

    // Keep both upstream transactions open for the duration of the rebuild so their streams/queries remain valid.
    cs.transaction { ctx =>
      ps.transaction { ptx =>
        val parents: Stream[Parent] = ptx.stream
        rebuild(
          parents = parents,
          childrenForParent = p =>
            ctx.query
              .filter((_: ChildModel) => Filter.Equals(joinField, p._id))
              .toList,
          truncateFirst = truncateFirst,
          commitEvery = commitEvery
        )
      }
    }.flatTap { _ =>
      optimize().when(optimizeAfterRebuild)
    }.map(_ => true)
  }
}

