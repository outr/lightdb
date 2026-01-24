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
   * Computes the next physical index name for an alias, using a numeric generation suffix if present.
   *
   * Example: alias "my_index" with existing "my_index_000001" -> "my_index_000002"
   */
  def nextPhysicalIndexName(readAlias: String,
                            existingIndices: List[String],
                            defaultSuffix: String = "_000001"): String = {
    // Prefer indices that look like "<alias>_<digits>" and choose the max generation.
    val Gen = """^(.*)_(\d+)$""".r
    val candidates = existingIndices.collect {
      case Gen(prefix, digits) if prefix == readAlias =>
        (digits.length, digits.toLong)
    }
    if candidates.nonEmpty then {
      val (width, maxGen) = candidates.maxBy(_._2)
      val next = maxGen + 1L
      s"${readAlias}_${String.format(s"%0${width}d", Long.box(next))}"
    } else {
      // If no generation suffix exists yet, use the configured default suffix (typically "_000001").
      s"$readAlias$defaultSuffix"
    }
  }

  /**
   * Creates a new physical index and atomically repoints the read alias and optional write alias to it.
   *
   * This does NOT reindex data; callers should rebuild/reindex into the new index before swapping if needed.
   *
   * @return the created physical index name
   */
  def createIndexAndRepointAliases(client: OpenSearchClient,
                                   readAlias: String,
                                   writeAlias: Option[String],
                                   indexBody: Json,
                                   defaultSuffix: String = "_000001"): Task[String] = {
    client.aliasTargets(readAlias).flatMap { existing =>
      val nextIndex = nextPhysicalIndexName(readAlias, existing, defaultSuffix = defaultSuffix)
      client.createIndex(nextIndex, indexBody).next {
        repointReadWriteAliases(client, readAlias, writeAlias, nextIndex).map(_ => nextIndex)
      }
    }
  }

  /**
   * Creates a new physical index, reindexes documents from the current read alias into it, then atomically repoints
   * the read alias and optional write alias.
   *
   * WARNING: This does not coordinate concurrent writes. For correctness under concurrent writes, callers must
   * pause writes or dual-write and perform a follow-up catch-up reindex before swapping.
   *
   * @return the new physical index name
   */
  def reindexAndRepointAliases(client: OpenSearchClient,
                               readAlias: String,
                               writeAlias: Option[String],
                               newIndexBody: Json,
                               defaultSuffix: String = "_000001"): Task[String] = {
    client.aliasTargets(readAlias).flatMap { existing =>
      val nextIndex = nextPhysicalIndexName(readAlias, existing, defaultSuffix = defaultSuffix)
      client.createIndex(nextIndex, newIndexBody)
        .next(client.reindex(source = readAlias, dest = nextIndex, refresh = true, waitForCompletion = true))
        .next(repointReadWriteAliases(client, readAlias, writeAlias, nextIndex))
        .map(_ => nextIndex)
    }
  }

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



