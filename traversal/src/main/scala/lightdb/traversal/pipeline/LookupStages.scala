package lightdb.traversal.pipeline

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.IndexingState
import lightdb.graph.EdgeDocument
import lightdb.id.Id
import lightdb.transaction.{CollectionTransaction, PrefixScanningTransaction, Transaction}
import lightdb.traversal.graph.{EdgeTraversalBuilder, TraversalStrategy}
import rapid.*

/**
 * MongoDB-style $lookup stages (correctness-first).
 *
 * These materialize the upstream, batch fetch the required ids, and then emit joined rows.
 */
object LookupStages {
  /**
   * Controls how many upstream rows are buffered at a time for lookup joins.
   *
   * This keeps lookup stages streaming-friendly while still doing batch-shaped work.
   */
  var DefaultLookupChunkSize: Int = 256

  def lookupOpt[A, Child <: Document[Child], ChildModel <: DocumentModel[Child]](
    childTx: Transaction[Child, ChildModel],
    id: A => Option[Id[Child]]
  ): Stage[A, (A, Option[Child])] =
    (in: Stream[A]) =>
      in.chunk(DefaultLookupChunkSize).flatMap { chunk =>
        Stream.force {
          val list = chunk.toList
          val ids = list.flatMap(a => id(a)).distinct
          ids.map { i =>
            childTx.get(i).map(opt => i.value -> opt)
          }.tasks.map(_.toMap).map { map =>
            Stream.emits(list.map(a => a -> id(a).flatMap(i => map.getOrElse(i.value, None))))
          }
        }
      }

  def lookupOne[A, Child <: Document[Child], ChildModel <: DocumentModel[Child]](
    childTx: Transaction[Child, ChildModel],
    id: A => Id[Child]
  ): Stage[A, (A, Option[Child])] =
    lookupOpt(childTx, a => Some(id(a)))

  /**
   * One-to-many lookup by foreign key (MongoDB $lookup style).
   *
   * Example: parent rows have `parentId`, child docs have `ownerId` field.
   */
  def lookupMany[A, Child <: Document[Child], ChildModel <: DocumentModel[Child], K](
    childTx: CollectionTransaction[Child, ChildModel],
    foreignKey: ChildModel => lightdb.field.Field[Child, K],
    key: A => K
  ): Stage[A, (A, List[Child])] =
    (in: Stream[A]) => {
      val fk = foreignKey(childTx.store.model)
      in.chunk(DefaultLookupChunkSize).flatMap { chunk =>
        Stream.force {
          val list = chunk.toList
          val keys = list.map(key).distinct

          // Stream children to avoid materializing all child docs for the chunk.
          val state = new IndexingState
          val grouped = scala.collection.mutable.HashMap.empty[K, scala.collection.mutable.ListBuffer[Child]]

          childTx.query
            .filter(_ => fk.in(keys))
            .stream
            .evalMap { child =>
              Task {
                val k = fk.get(child, fk, state).asInstanceOf[K]
                grouped.getOrElseUpdate(k, scala.collection.mutable.ListBuffer.empty).append(child)
              }
            }
            .drain
            .map { _ =>
              val groupedMap: Map[K, List[Child]] = grouped.iterator.map { case (k, lb) => k -> lb.toList }.toMap
              Stream.emits(list.map(a => a -> groupedMap.getOrElse(key(a), Nil)))
            }
        }
      }
    }

  def lookupManyField[A, Child <: Document[Child], ChildModel <: DocumentModel[Child], K](
    childTx: CollectionTransaction[Child, ChildModel],
    foreignKey: lightdb.field.Field[Child, K],
    key: A => K
  ): Stage[A, (A, List[Child])] =
    lookupMany(childTx, (_: ChildModel) => foreignKey, key)

  /**
   * Recursive multi-hop join, analogous to MongoDB's `$graphLookup`.
   *
   * For each upstream row, starts a graph traversal from `startWith(row)` and collects
   * all edges reachable within `maxDepth` hops. Results are emitted as `(row, List[E])`.
   *
   * @param edgeTx     Transaction for the edge collection (must support prefix scanning)
   * @param startWith  Extracts the starting node ID from each upstream row
   * @param maxDepth   Maximum traversal depth (default: Int.MaxValue)
   * @param edgeFilter Optional predicate to filter edges during traversal
   * @param strategy   Traversal strategy (BFS or DFS, default: BFS)
   */
  def graphLookup[A, E <: EdgeDocument[E, N, N], N <: Document[N]](
    edgeTx: PrefixScanningTransaction[E, _],
    startWith: A => Id[N],
    maxDepth: Int = Int.MaxValue,
    edgeFilter: E => Boolean = (_: E) => true,
    strategy: TraversalStrategy = TraversalStrategy.BFS
  ): Stage[A, (A, List[E])] =
    (in: Stream[A]) =>
      in.chunk(DefaultLookupChunkSize).flatMap { chunk =>
        Stream.force {
          val list = chunk.toList
          list.map { a =>
            val startId = startWith(a)
            EdgeTraversalBuilder(
              fromIds = Stream.emit(startId),
              tx = edgeTx,
              maxDepth = maxDepth,
              edgeFilter = edgeFilter,
              strategy = strategy
            ).edges.toList.map(edges => a -> edges)
          }.tasks.map(pairs => Stream.emits(pairs))
        }
      }
}


