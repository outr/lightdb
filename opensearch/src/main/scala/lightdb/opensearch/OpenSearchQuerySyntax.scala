package lightdb.opensearch

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.{Query, Sort}
import lightdb.opensearch.util.OpenSearchCursor
import rapid.Task
import rapid.Stream

object OpenSearchQuerySyntax {
  private def splitSearching(tx: AnyRef): Option[Any] =
    try {
      val m = tx.getClass.getMethod("searching")
      Some(m.invoke(tx))
    } catch {
      case _: Throwable => None
    }

  implicit class OpenSearchGroupingOps[Doc <: Document[Doc], Model <: DocumentModel[Doc], V](private val query: Query[Doc, Model, V]) extends AnyVal {
    def groupBy[G](field: Model => Field[Doc, G],
                   docsPerGroup: Option[Int] = None,
                   groupOffset: Option[Int] = None,
                   groupLimit: Option[Int] = None,
                   groupSort: Option[List[Sort]] = None,
                   withinGroupSort: Option[List[Sort]] = None,
                   includeScores: Boolean = query.scoreDocs,
                   includeTotalGroupCount: Boolean = true): Task[OpenSearchGroupedSearchResults[Doc, Model, G, V]] = query.transaction match {
      case tx: OpenSearchTransaction[Doc, Model] =>
        val resolvedDocsPerGroup = docsPerGroup.orElse(query.pageSize).getOrElse(1)
        val resolvedGroupOffset = groupOffset.getOrElse(query.offset)
        val resolvedGroupLimit = groupLimit.orElse(query.limit)
        val resolvedGroupSort = groupSort.getOrElse(query.sort)
        val resolvedWithinGroupSort = withinGroupSort.getOrElse(query.sort)
        tx.groupBy(
          query = query,
          groupField = field(query.model),
          docsPerGroup = resolvedDocsPerGroup,
          groupOffset = resolvedGroupOffset,
          groupLimit = resolvedGroupLimit,
          groupSort = resolvedGroupSort,
          withinGroupSort = resolvedWithinGroupSort,
          includeScores = includeScores,
          includeTotalGroupCount = includeTotalGroupCount
        )
      case _ =>
        Task.error(new RuntimeException("OpenSearch grouping is only supported when using an OpenSearch-backed collection"))
    }
  }

  implicit class OpenSearchCursorOps[Doc <: Document[Doc], Model <: DocumentModel[Doc], V](private val query: Query[Doc, Model, V]) extends AnyVal {
    /**
     * Executes a cursor (keyset) pagination page using OpenSearch `search_after`.
     *
     * - `cursorToken=None` returns the first page
     * - `cursorToken=Some(token)` resumes from the previous page's `nextCursorToken`
     *
     * Note: This is OpenSearch-specific for now (does not change LightDB core Query API).
     */
    def cursorPage(cursorToken: Option[String] = None,
                   pageSize: Int = query.pageSize.getOrElse(25)): Task[OpenSearchCursorPage[Doc, Model, V]] = query.transaction match {
      case tx: OpenSearchTransaction[Doc, Model] =>
        val searchAfter = cursorToken.flatMap(OpenSearchCursor.decode)
        tx.doSearchAfter(query = query, searchAfter = searchAfter, pageSize = pageSize)
      case tx: AnyRef if tx.getClass.getName.contains("SplitCollectionTransaction") =>
        splitSearching(tx) match {
          case Some(searchingAny) if searchingAny.isInstanceOf[OpenSearchTransaction[_, _]] =>
            val os = searchingAny.asInstanceOf[OpenSearchTransaction[Doc, Model]]
            val searchAfter = cursorToken.flatMap(OpenSearchCursor.decode)
            os.doSearchAfter(query = query, searchAfter = searchAfter, pageSize = pageSize)
          case _ =>
            Task.error(new RuntimeException("OpenSearch cursor pagination is only supported when using an OpenSearch-backed collection"))
        }
      case _ =>
        Task.error(new RuntimeException("OpenSearch cursor pagination is only supported when using an OpenSearch-backed collection"))
    }

    /**
     * Returns a stream of results using OpenSearch cursor pagination (`search_after`) when supported.
     *
     * If the underlying collection is not OpenSearch-backed, this falls back to LightDB's default `Query.stream`.
     */
    def cursorStream(pageSize: Int = query.pageSize.getOrElse(1000)): Stream[V] = {
      def loop(token: Option[String]): Stream[V] =
        Stream.force {
          cursorPage(cursorToken = token, pageSize = pageSize).map { page =>
            val current = page.results.stream
            page.nextCursorToken match {
              case Some(next) => current.append(loop(Some(next)))
              case None => current
            }
          }
        }

      query.transaction match {
        case _: OpenSearchTransaction[_, _] => loop(None)
        case tx: AnyRef if tx.getClass.getName.contains("SplitCollectionTransaction") =>
          splitSearching(tx) match {
            case Some(searchingAny) if searchingAny.isInstanceOf[OpenSearchTransaction[_, _]] => loop(None)
            case _ => query.stream
          }
        case _ => query.stream
      }
    }
  }
}


