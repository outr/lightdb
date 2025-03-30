package lightdb.store.sharded

import fabric.Json
import lightdb._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.facet.{FacetResult, FacetResultValue}
import lightdb.field.Field._
import lightdb.field.{Field, IndexingState}
import lightdb.materialized.MaterializedAggregate
import lightdb.store.sharded.manager.{ShardManager, ShardManagerInstance}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.util.JsonOrdering
import rapid._

import scala.language.implicitConversions

/**
 * A Store implementation that distributes data across multiple shards.
 *
 * @param name         The name of the store
 * @param model        The document model
 * @param shardManager The shard manager
 * @param storeMode    The store mode
 * @param storeManager The store manager
 */
class ShardedStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](override val name: String,
                                                                      model: Model,
                                                                      shardManager: ShardManagerInstance[Doc, Model],
                                                                      val storeMode: StoreMode[Doc, Model],
                                                                      db: LightDB,
                                                                      storeManager: StoreManager) extends Store[Doc, Model](name, model, db, storeManager) {
  override protected def initialize(): Task[Unit] = super.initialize().next {
    shardManager.shards.foldLeft(Task.unit) { (task, shard) =>
      task.flatMap(_ => shard.init)
    }
  }

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = {
    shardManager.shards.foldLeft(Task.unit) { (task, shard) =>
      task.flatMap(_ => shard.prepareTransaction(transaction))
    }
  }

  override protected def _insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = shardManager.insert(doc)

  override protected def _upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = shardManager.upsert(doc)

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = shardManager.exists(id)

  override protected def _get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = {
    val findFirst = () => shardManager.shards.foldLeft(Task.pure(Option.empty[Doc])) { (task, shard) =>
      task.flatMap {
        case Some(doc) => Task.pure(Some(doc))
        case None => shard.get(_ => field -> value)
      }
    }
    if (field == idField) {
      val id = value.asInstanceOf[Id[Doc]]
      shardManager.shardFor(id) match {
        case Some(store) => store.get(_ => field -> value)
        case None => findFirst()
      }
    } else {
      findFirst()
    }
  }

  override protected def _delete[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Boolean] =
    shardManager.delete(field, value).map(_.nonEmpty)

  override def count(implicit transaction: Transaction[Doc]): Task[Int] =
    shardManager.shards.foldLeft(Task.pure(0)) { (task, shard) =>
      task.flatMap { count =>
        shard.count.map(_ + count)
      }
    }

  def shardCounts(implicit transaction: Transaction[Doc]): Task[Vector[Int]] =
    shardManager.shards.map(_.count).tasks.map(_.toVector)

  override def stream(implicit transaction: Transaction[Doc]): Stream[Doc] =
    Stream.fromIterator(Task.pure(shardManager.shards.iterator)).flatMap(_.stream)

  override def jsonStream(implicit transaction: Transaction[Doc]): Stream[Json] =
    Stream.fromIterator(Task.pure(shardManager.shards.iterator)).flatMap(_.jsonStream)

  override def doSearch[V](query: Query[Doc, Model, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] = {
    // TODO: Optimize this to more efficiently query
    val offset = 0
    val limit = query.limit.map(l => query.offset + l)
    val pageSize = query.pageSize * shardManager.shards.length
    val results = shardManager.shards.map(_.doSearch(query.copy(offset = offset, limit = limit, pageSize = pageSize)))

    // TODO: Concurrent!
    results.foldLeft(Task.pure(Option.empty[SearchResults[Doc, Model, V]])) { (task, resultTask) =>
      task.flatMap { optResult =>
        resultTask.flatMap { result =>
          optResult match {
            case Some(existing) => mergeSearchResults(existing, result)
            case None => Task.pure(Some(result))
          }
        }
      }
    }.flatMap {
      case Some(result) =>
        result.streamWithScore.toList.map { list =>
          val sortedList: List[(V, Double)] = if (query.sort.nonEmpty) {
            val sort = query.sort.head
            sort match {
              case Sort.BestMatch(direction) =>
                val sorted = list.sortBy(_._2)
                if (direction == SortDirection.Descending) sorted.reverse else sorted
              case Sort.IndexOrder => list
              case Sort.ByField(field, direction) if list.nonEmpty =>
                val indexingState = new IndexingState
                val f = field.asInstanceOf[Field[Doc, _]]
                if (list.head._1.isInstanceOf[Document[_]]) {
                  val ordering = direction match {
                    case SortDirection.Ascending => JsonOrdering
                    case SortDirection.Descending => JsonOrdering.reverse
                  }
                  list.asInstanceOf[List[(Doc, Double)]].sortBy(t => f.getJson(t._1, indexingState))(ordering).asInstanceOf[List[(V, Double)]]
                } else {
                  scribe.info(s"Unknown type: ${list.head._1.getClass.getName}")
                  list
                }
              case _ => list
            }
          } else {
            // Default to sorting by score (descending)
            list.sortBy(-_._2)
          }

          val limitedList = query.limit match {
            case Some(limit) => sortedList.slice(query.offset, query.offset + limit).take(query.pageSize)
            case None => sortedList.slice(query.offset, query.offset + query.pageSize)
          }

          // Create the final search results with the sorted and limited list
          SearchResults(
            model = result.model,
            offset = query.offset,
            limit = query.limit,
            total = result.total,
            streamWithScore = Stream.fromIterator(Task.pure(limitedList.iterator)),
            facetResults = result.facetResults,
            transaction = transaction
          )
        }
      case None =>
        // If there are no results, return empty results
        Task.pure(SearchResults(
          model = model,
          offset = query.offset,
          limit = query.limit,
          total = Some(0),
          streamWithScore = Stream.empty,
          facetResults = Map.empty,
          transaction = transaction
        ))
    }
  }

  /**
   * Merges two search results.
   *
   * @param result1 The first search result
   * @param result2 The second search result
   * @return The merged search result
   */
  private def mergeSearchResults[V](result1: SearchResults[Doc, Model, V],
                                    result2: SearchResults[Doc, Model, V]): Task[Option[SearchResults[Doc, Model, V]]] = {
    // Calculate the total count
    val total = (result1.total, result2.total) match {
      case (Some(t1), Some(t2)) => Some(t1 + t2)
      case _ => None
    }

    // Merge the facet results
    val facetResults = mergeFacetResults(result1.facetResults, result2.facetResults)

    // Merge the streams with scores
    val mergedStream = result1.streamWithScore ++ result2.streamWithScore

    // Create the merged search results
    Task.pure(Some(SearchResults(
      model = model,
      offset = result1.offset,
      limit = result1.limit,
      total = total,
      streamWithScore = mergedStream,
      facetResults = facetResults,
      transaction = result1.transaction
    )))
  }

  /**
   * Merges two facet result maps.
   *
   * @param facetResults1 The first facet result map
   * @param facetResults2 The second facet result map
   * @return The merged facet result map
   */
  private def mergeFacetResults(facetResults1: Map[FacetField[Doc], FacetResult],
                                facetResults2: Map[FacetField[Doc], FacetResult]): Map[FacetField[Doc], FacetResult] = {
    // Combine the keys from both maps
    val allKeys = facetResults1.keySet ++ facetResults2.keySet

    // For each key, merge the facet results
    allKeys.map { field =>
      val result1 = facetResults1.getOrElse(field, FacetResult(Nil, 0, 0))
      val result2 = facetResults2.getOrElse(field, FacetResult(Nil, 0, 0))

      // Merge the values
      val valueMap = (result1.values ++ result2.values).groupBy(_.value).map {
        case (value, values) =>
          val count = values.map(_.count).sum
          FacetResultValue(value, count)
      }.toList

      // Calculate the total count and child count
      val totalCount = valueMap.map(_.count).sum
      val childCount = valueMap.size

      // Create the merged facet result
      field -> FacetResult(valueMap, childCount, totalCount)
    }.toMap
  }

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): Stream[MaterializedAggregate[Doc, Model]] = {
    // Aggregate across all shards
    // Note: This is a simplified implementation that concatenates the aggregation results from all shards
    // A more complete implementation would merge the aggregation results
    Stream.fromIterator(Task.pure(shardManager.shards.iterator)).flatMap(_.aggregate(query))
  }

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int] = {
    // Sum the aggregation counts from all shards
    shardManager.shards.foldLeft(Task.pure(0)) { (task, shard) =>
      task.flatMap { count =>
        shard.aggregateCount(query).map(_ + count)
      }
    }
  }

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = {
    // Truncate all shards and sum the counts
    shardManager.shards.foldLeft(Task.pure(0)) { (task, shard) =>
      task.flatMap { count =>
        shard.truncate().map(_ + count)
      }
    }
  }

  override def verify(): Task[Boolean] = {
    // Verify all shards and return true if all verifications succeed
    shardManager.shards.foldLeft(Task.pure(true)) { (task, shard) =>
      task.flatMap { result =>
        if (!result) {
          Task.pure(false)
        } else {
          shard.verify()
        }
      }
    }
  }

  override def reIndex(): Task[Boolean] = shardManager.shards.map { store =>
    store.reIndex()
  }.tasks.map(_ => true)

  override def reIndex(doc: Doc): Task[Boolean] = transaction { implicit transaction =>
    shardManager.reIndex(doc)
  }

  override def optimize(): Task[Unit] = {
    // Optimize all shards
    shardManager.shards.foldLeft(Task.unit) { (task, shard) =>
      task.flatMap(_ => shard.optimize())
    }
  }

  override protected def doDispose(): Task[Unit] = super.doDispose().next {
    // Dispose all shards
    shardManager.shards.foldLeft(Task.unit) { (task, shard) =>
      task.flatMap(_ => shard.dispose)
    }
  }
}
