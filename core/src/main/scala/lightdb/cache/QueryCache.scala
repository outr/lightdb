package lightdb.cache

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.time.Timestamp
import lightdb.{Query, SearchResults}
import rapid.{Fiber, Task}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * QueryCache system to cache frequently used queries.
 */
trait QueryCache {
  type Key
  type Doc <: Document[Doc]
  type Model <: DocumentModel[Doc]
  type V

  protected def id(v: V): Id[Doc]

  /**
   * Cached results timeout. Defaults to 30 minutes.
   */
  protected def timeout: Option[FiniteDuration] = Some(30.minutes)

  /**
   * Maximum number of entries able to be cached before releasing. Defaults to 100.
   */
  protected def maxEntries: Option[Int] = Some(100)

  /**
   * Whether to only cache the first page of queries. Defaults to true.
   */
  protected def onlyFirstPage: Boolean = true

  /**
   * If set to false, acts as just a pass-through. Defaults to true.
   */
  protected def enabled: Boolean = true

  private val map = new ConcurrentHashMap[(Key, Query[Doc, Model, V]), Cached]

  /**
   * Retrieve or start the search for `query`. If it's already in the map,
   * we update its usage (increment reference count, update timestamp).
   */
  def apply(key: Key, query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] = if (!enabled) {
    query.search
  } else if (onlyFirstPage && query.offset > 0) {
    query.search
  } else {
    map.computeIfAbsent(key -> query, _ => {
      val fiber = query.search.flatMap { results =>
        results.streamWithScore.toList.map { list =>
          results.copy(streamWithScore = rapid.Stream.emits(list))
        }
      }.start()
      new Cached(fiber)
    }).incrementUsage().fiber
  }

  /**
   * Updates all cached entries that contain the document with the same Key and Id as the provided value.
   * The updated value replaces the old one while maintaining the original score.
   *
   * @param key The key associated with the cache entries
   * @param v The new value to update with
   */
  def modify(key: Key, v: V): Task[Unit] = Task {
    val docId = id(v)
    map.entrySet().asScala.filter(_.getKey._1 == key).foreach { entry =>
      val fiber = entry.getValue.fiber.flatMap { results =>
        results.streamWithScore.toList.map { list =>
          if (list.exists { case (value, _) => id(value) == docId }) {
            val updatedList = list.map { case (value, score) =>
              if (id(value) == docId) (v, score) else (value, score)
            }
            results.copy(streamWithScore = rapid.Stream.emits(updatedList))
          } else {
            results
          }
        }
      }.start()
      entry.setValue(new Cached(fiber))
    }
  }

  def clear(): Task[Unit] = Task {
    map.clear()
  }

  /**
   * Clean out expired entries, then remove entries if maxEntries is reached.
   * Strategy for removal when exceeding maxEntries:
   * - Sort by reference count ascending, then by last-access time ascending,
   *   so that least-used and oldest get removed first.
   */
  def clean(repeatEvery: Option[FiniteDuration]): Task[Unit] = {
    val task = Task {
      // Remove timed-out entries
      timeout.foreach { to =>
        val iterator = map.entrySet().iterator()
        while (iterator.hasNext) {
          val entry = iterator.next()
          val cached = entry.getValue
          if (cached.lastAccess.isExpired(to)) {
            iterator.remove()
          }
        }
      }

      // Clear out old and less used entries
      maxEntries.foreach { max =>
        val size = map.size()
        if (size > max) {
          val removeCount = size - max
          map.entrySet().asScala.toList.sortBy { entry =>
            val cached = entry.getValue
            (cached.referenceCount, cached.lastAccess.value)
          }.take(removeCount).foreach { entry =>
            map.remove(entry.getKey)
          }
        }
      }
    }
    repeatEvery match {
      case Some(duration) => Task.sleep(duration).next(task).next(clean(repeatEvery))
      case None => task
    }
  }

  private class Cached(val fiber: Fiber[SearchResults[Doc, Model, V]]) {
    @volatile private var _lastAccess = Timestamp()
    @volatile private var _referenceCount = 0

    def lastAccess: Timestamp = _lastAccess
    def referenceCount: Int = _referenceCount

    def incrementUsage(): Cached = synchronized {
      _lastAccess = Timestamp()
      _referenceCount += 1
      this
    }
  }
}
