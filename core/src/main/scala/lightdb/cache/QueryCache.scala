package lightdb.cache

import lightdb.{Query, SearchResults, Timestamp}
import lightdb.doc.{Document, DocumentModel}
import lightdb.transaction.Transaction
import rapid.{Fiber, Task}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * QueryCache system to cache frequently used queries.
 *
 * @param timeout defaults to 30 minutes
 * @param maxEntries defaults to 100
 * @param onlyFirstPage defaults to true
 */
case class QueryCache[Doc <: Document[Doc], Model <: DocumentModel[Doc], V](timeout: Option[FiniteDuration] = Some(30.minutes),
                                                                            maxEntries: Option[Int] = Some(100),
                                                                            onlyFirstPage: Boolean = true) {
  private val map = new ConcurrentHashMap[Query[Doc, Model, V], Cached]

  /**
   * Retrieve or start the search for `query`. If it's already in the map,
   * we update its usage (increment reference count, update timestamp).
   */
  def apply(query: Query[Doc, Model, V])
           (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] = if (onlyFirstPage && query.offset > 0) {
    query.search
  } else {
    map.computeIfAbsent(query, _ => {
      val fiber = query.search.flatMap { results =>
        results.streamWithScore.toList.map { list =>
          results.copy(streamWithScore = rapid.Stream.emits(list))
        }
      }.start()
      new Cached(fiber)
    }).incrementUsage().fiber
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