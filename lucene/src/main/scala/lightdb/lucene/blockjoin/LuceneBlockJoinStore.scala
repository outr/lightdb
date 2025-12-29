package lightdb.lucene.blockjoin

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, ParentChildSupport}
import lightdb.field.Field
import lightdb.lucene.LuceneStore
import lightdb.store.{CollectionManager, StoreMode}
import org.apache.lucene.search.IndexSearcher
import rapid.Task
import rapid.Stream

import java.nio.file.Path

/**
 * Lucene store that supports parent/child queries via Lucene block-join.
 *
 * This store is intended primarily as a generated search index:
 * - Parent docs are indexed with "p." field prefixes
 * - Child docs are indexed with "c." field prefixes
 * - Child docs are indexed before the parent doc as a single block
 *
 * Query-time joins are compiled into ToParentBlockJoinQuery.
 */
class LuceneBlockJoinStore[
  Parent <: Document[Parent],
  Child <: Document[Child],
  ChildModel <: DocumentModel[Child],
  ParentModel <: ParentChildSupport[Parent, Child, ChildModel]
](name: String,
  path: Option[Path],
  model: ParentModel,
  override val storeMode: StoreMode[Parent, ParentModel],
  lightDB: LightDB,
  storeManager: CollectionManager) extends LuceneStore[Parent, ParentModel](name, path, model, storeMode, lightDB, storeManager) {

  IndexSearcher.setMaxClauseCount(10_000_000)

  override def supportsNativeExistsChild: Boolean = true

  /**
   * Controls which PARENT fields are indexed into the block-join Lucene index.
   *
   * Defaults to all fields that this store would normally index (i.e. `store.fields` which respects StoreMode.Indexes).
   * You can narrow this to keep the generated index small.
   *
   * NOTE: `_id` is always included regardless of this filter.
   */
  @volatile var parentFieldFilter: Field[Parent, _] => Boolean = _ => true

  /**
   * Controls which CHILD fields are indexed into the block-join Lucene index.
   *
   * Defaults to indexed-only fields (to avoid accidental huge payload indexing).
   *
   * NOTE: child `_id` is always included regardless of this filter.
   */
  @volatile var childFieldFilter: Field[Child, _] => Boolean = _.indexed

  /**
   * When true, child documents will be stored similarly to StoreMode.All (in addition to indexing).
   * Default is false: child docs are not intended to be materialized from the search index.
   */
  @volatile var childStoreAll: Boolean = false

  private def selectedParentFields: List[Field[Parent, _]] = {
    val base = fields.filter(parentFieldFilter)
    val id = model._id.asInstanceOf[Field[Parent, _]]
    (id :: base).distinct
  }

  private def selectedChildFields: List[Field[Child, _]] = {
    val base = model.childStore.model.fields.asInstanceOf[List[Field[Child, _]]].filter(childFieldFilter)
    val id = model.childStore.model._id.asInstanceOf[Field[Child, _]]
    (id :: base).distinct
  }

  override def optimize(): Task[Unit] = Task {
    // merge to a single segment for best join performance
    index.indexWriter.forceMerge(1)
  }

  /**
   * Adds a single Lucene block: children first, parent last.
   *
   * IMPORTANT: do not use normal `insert`/`upsert` for this store for parent/child use-cases; those do not
   * produce contiguous blocks and block-join queries will not work correctly.
   */
  def indexBlock(parent: Parent, children: List[Child]): Task[Unit] =
    LuceneBlockJoinIndexer.indexBlock(
      indexWriter = index.indexWriter,
      parentModel = model,
      parentFields = selectedParentFields,
      parentStoreAll = storeMode.isAll,
      parent = parent,
      childModel = model.childStore.model,
      childFields = selectedChildFields,
      // Child docs are not intended to be materialized; avoid storing full child payloads by default.
      childStoreAll = childStoreAll,
      children = children
    )

  /** Commits the underlying Lucene index so subsequent queries see newly indexed blocks. */
  def commitIndex(): Task[Unit] = Task(index.commit())

  /**
   * Convenience helper for generated indexing:
   * - optionally truncates the index
   * - indexes one block per parent (children then parent)
   * - commits at the end
   *
   * This is intentionally sequential by default to keep IndexWriter usage simple and deterministic.
   */
  def rebuild(parents: Stream[Parent],
              childrenForParent: Parent => Task[List[Child]],
              truncateFirst: Boolean = true,
              commitEvery: Int = 0): Task[Unit] = {
    val start: Task[Unit] =
      if (truncateFirst) Task(index.indexWriter.deleteAll()).unit else Task.unit

    start.next {
      var n = 0
      parents.evalMap { p =>
        childrenForParent(p).flatMap { kids =>
          indexBlock(p, kids).flatTap { _ =>
            n += 1
            if (commitEvery > 0 && n % commitEvery == 0) commitIndex() else Task.unit
          }
        }
      }.drain.next(commitIndex())
    }
  }

  /**
   * Rebuild helper for the common LN-style pattern:
   * - enumerate child ids for a parent
   * - load children by id (fast when childStore is RocksDB or SplitStore storage-backed)
   */
  def rebuildFromChildIds(parents: Stream[Parent],
                          childIdsForParent: Parent => Task[List[lightdb.id.Id[Child]]],
                          childChunkSize: Int = 10_000,
                          truncateFirst: Boolean = true,
                          commitEvery: Int = 0): Task[Unit] = rebuild(
    parents = parents,
    childrenForParent = p =>
      childIdsForParent(p).flatMap { ids =>
        // Transaction.getAll is default per-id, but stores like RocksDB override it efficiently.
        model.childStore.transaction { tx =>
          rapid.Stream
            .emits(ids)
            .chunk(childChunkSize)
            .evalMap(chunk => tx.getAll(chunk).toList)
            .flatMap(list => rapid.Stream.emits(list))
            .toList
        }
      },
    truncateFirst = truncateFirst,
    commitEvery = commitEvery
  )
}


