package lightdb.store.sharded

import fabric.Json
import lightdb._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.facet.{FacetResult, FacetResultValue}
import lightdb.field.Field._
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import rapid.{Stream, Task, logger}

import scala.language.implicitConversions

/**
 * A Store implementation that distributes data across multiple shards.
 *
 * @param name         The name of the store
 * @param model        The document model
 * @param shards       The shards to distribute data across
 * @param storeMode    The store mode
 * @param storeManager The store manager
 */
class ShardedStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](override val name: String,
                                                                      model: Model,
                                                                      shards: Vector[Store[Doc, Model]],
                                                                      val storeMode: StoreMode[Doc, Model],
                                                                      storeManager: StoreManager) extends Store[Doc, Model](name, model, storeManager) {

  /**
   * Determines which shard to use for a given document ID.
   *
   * @param id The document ID
   * @return The shard index
   */
  private def shardIndex(id: Id[Doc]): Int = {
    // Simple hash-based sharding
    Math.abs(id.value.hashCode() % shards.size)
  }

  /**
   * Gets the shard for a given document ID.
   *
   * @param id The document ID
   * @return The shard
   */
  private def shardFor(id: Id[Doc]): Store[Doc, Model] = {
    shards(shardIndex(id))
  }

  override protected def initialize(): Task[Unit] = {
    // Initialize all shards
    shards.foldLeft(Task.unit) { (task, shard) =>
      task.flatMap(_ => shard.init)
    }
  }

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = {
    // Prepare transaction for all shards
    shards.foldLeft(Task.unit) { (task, shard) =>
      task.flatMap(_ => shard.prepareTransaction(transaction))
    }
  }

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = {
    // Insert into the appropriate shard based on document ID
    shardFor(id(doc)).insert(doc)
  }

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = {
    // Upsert into the appropriate shard based on document ID
    shardFor(id(doc)).upsert(doc)
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = {
    // Check if document exists in the appropriate shard
    shardFor(id).exists(id)
  }

  override def get[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Option[Doc]] = {
    if (field == idField) {
      // If we're querying by ID, we can go directly to the appropriate shard
      val id = value.asInstanceOf[Id[Doc]]
      shardFor(id).get(field, value)
    } else {
      // Otherwise, we need to query all shards and take the first result
      // We'll do this by folding over the shards and returning the first result we find
      shards.foldLeft(Task.pure(Option.empty[Doc])) { (task, shard) =>
        task.flatMap {
          case Some(doc) => Task.pure(Some(doc))
          case None => shard.get(field, value)
        }
      }
    }
  }

  override def delete[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Boolean] = {
    if (field == idField) {
      // If we're deleting by ID, we can go directly to the appropriate shard
      val id = value.asInstanceOf[Id[Doc]]
      shardFor(id).delete(field, value)
    } else {
      // Otherwise, we need to delete from all shards and return true if any deletion succeeded
      // We'll do this by folding over the shards and returning true if any deletion succeeded
      shards.foldLeft(Task.pure(false)) { (task, shard) =>
        task.flatMap { result =>
          if (result) {
            Task.pure(true)
          } else {
            shard.delete(field, value)
          }
        }
      }
    }
  }

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = {
    // Sum the counts from all shards
    shards.foldLeft(Task.pure(0)) { (task, shard) =>
      task.flatMap { count =>
        shard.count.map(_ + count)
      }
    }
  }

  def shardCounts(implicit transaction: Transaction[Doc]): Task[Vector[Int]] = {
    shards.map(_.count).tasks.map(_.toVector)
  }

  override def stream(implicit transaction: Transaction[Doc]): Stream[Doc] = {
    // Concatenate streams from all shards
    Stream.fromIterator(Task.pure(shards.iterator)).flatMap(_.stream)
  }

  override def jsonStream(implicit transaction: Transaction[Doc]): Stream[Json] = {
    // Concatenate JSON streams from all shards
    Stream.fromIterator(Task.pure(shards.iterator)).flatMap(_.jsonStream)
  }

  override def doSearch[V](query: Query[Doc, Model, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] = {
    // Execute the query on all shards
    val results = shards.map(_.doSearch(query))

    // Merge the results
    results.foldLeft(Task.pure(Option.empty[SearchResults[Doc, Model, V]])) { (task, resultTask) =>
      task.flatMap { optResult =>
        resultTask.flatMap { result =>
          optResult match {
            case Some(existing) => mergeSearchResults(existing, result)
            case None => Task.pure(Some(result))
          }
        }
      }
    }.flatMap { optResult =>
      optResult match {
        case Some(result) =>
          // Apply sorting and limiting to the merged results
          result.streamWithScore.toList.map { list =>
            // Sort the list by score (descending by default)
            val sortedList = if (query.sort.nonEmpty) {
              // Sort by the first sort criterion
              val sort = query.sort.head
              sort match {
                case Sort.BestMatch(direction) =>
                  // Sort by score
                  val sorted = list.sortBy(_._2)
                  if (direction == SortDirection.Descending) sorted.reverse else sorted
                case Sort.IndexOrder =>
                  // No sorting needed
                  list
                case _ =>
                  // For other sort criteria, we can't sort here because we don't have access to the document fields
                  // Just return the list as-is
                  list
              }
            } else {
              // Default to sorting by score (descending)
              list.sortBy(-_._2)
            }

            // Apply the limit if specified
            val limitedList = query.limit match {
              case Some(limit) => sortedList.take(limit)
              case None => sortedList
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
    Stream.fromIterator(Task.pure(shards.iterator)).flatMap(_.aggregate(query))
  }

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int] = {
    // Sum the aggregation counts from all shards
    shards.foldLeft(Task.pure(0)) { (task, shard) =>
      task.flatMap { count =>
        shard.aggregateCount(query).map(_ + count)
      }
    }
  }

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = {
    // Truncate all shards and sum the counts
    shards.foldLeft(Task.pure(0)) { (task, shard) =>
      task.flatMap { count =>
        shard.truncate().map(_ + count)
      }
    }
  }

  override def verify(): Task[Boolean] = {
    // Verify all shards and return true if all verifications succeed
    shards.foldLeft(Task.pure(true)) { (task, shard) =>
      task.flatMap { result =>
        if (!result) {
          Task.pure(false)
        } else {
          shard.verify()
        }
      }
    }
  }

  override def reIndex(): Task[Boolean] = {
    // Re-index all shards and return true if all re-indexings succeed
    shards.foldLeft(Task.pure(true)) { (task, shard) =>
      task.flatMap { result =>
        if (!result) {
          Task.pure(false)
        } else {
          shard.reIndex()
        }
      }
    }
  }

  override def reIndex(doc: Doc): Task[Boolean] = {
    // Re-index the document in the appropriate shard
    shardFor(id(doc)).reIndex(doc)
  }

  override def optimize(): Task[Unit] = {
    // Optimize all shards
    shards.foldLeft(Task.unit) { (task, shard) =>
      task.flatMap(_ => shard.optimize())
    }
  }

  override protected def doDispose(): Task[Unit] = {
    // Dispose all shards
    shards.foldLeft(Task.unit) { (task, shard) =>
      task.flatMap(_ => shard.dispose)
    }
  }
}
