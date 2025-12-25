package lightdb.opensearch

import fabric._
import lightdb.opensearch.client.OpenSearchClient
import rapid.Task

/**
 * Helper utilities for alias-based index migrations.
 *
 * This intentionally does not perform reindexing; it only manages aliases safely/atomically.
 */
object OpenSearchIndexMigration {
  /**
   * Atomically repoint an alias to a single target index, removing the alias from any previous indices.
   */
  def repointAlias(client: OpenSearchClient, alias: String, targetIndex: String): Task[Unit] =
    client.aliasTargets(alias).flatMap { existing =>
      val removes = existing.map(idx => obj("remove" -> obj("index" -> str(idx), "alias" -> str(alias))))
      val add = obj("add" -> obj("index" -> str(targetIndex), "alias" -> str(alias)))
      client.updateAliases(obj("actions" -> arr((removes :+ add): _*)))
    }

  /**
   * Atomically repoint a read alias and (optionally) a write alias to a single target index.
   *
   * If writeAlias is provided, it is added with `is_write_index=true` and removed from any previous indices.
   */
  def repointReadWriteAliases(client: OpenSearchClient,
                              readAlias: String,
                              writeAlias: Option[String],
                              targetIndex: String): Task[Unit] = {
    val write = writeAlias.toList
    val allAliases = (readAlias :: write).distinct
    allAliases.tasksFlatMap { a =>
      client.aliasTargets(a).map(a -> _)
    }.flatMap { pairs =>
      val removeActions = pairs.flatMap { case (a, indices) =>
        indices.map(idx => obj("remove" -> obj("index" -> str(idx), "alias" -> str(a))))
      }
      val addRead = obj("add" -> obj("index" -> str(targetIndex), "alias" -> str(readAlias)))
      val addWrite = writeAlias.map { wa =>
        obj("add" -> obj("index" -> str(targetIndex), "alias" -> str(wa), "is_write_index" -> bool(true)))
      }
      val actions = removeActions ++ (addRead :: addWrite.toList)
      client.updateAliases(obj("actions" -> arr(actions: _*)))
    }
  }

  // Helper to sequence tasks without requiring additional imports at call sites.
  private implicit class TaskListOps[A](private val list: List[A]) extends AnyVal {
    def tasksFlatMap[B](f: A => Task[B]): Task[List[B]] =
      list.foldLeft(Task.pure(List.empty[B])) { (acc, a) =>
        acc.flatMap(bs => f(a).map(b => bs :+ b))
      }
  }
}



