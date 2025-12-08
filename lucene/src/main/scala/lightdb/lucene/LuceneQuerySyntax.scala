package lightdb.lucene

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.{Query, Sort}
import rapid.Task

object LuceneQuerySyntax {
  implicit class LuceneGroupingOps[Doc <: Document[Doc], Model <: DocumentModel[Doc], V](private val query: Query[Doc, Model, V]) extends AnyVal {
    def groupBy[G](field: Model => Field[Doc, G],
                   docsPerGroup: Option[Int] = None,
                   groupOffset: Option[Int] = None,
                   groupLimit: Option[Int] = None,
                   groupSort: Option[List[Sort]] = None,
                   withinGroupSort: Option[List[Sort]] = None,
                   includeScores: Boolean = query.scoreDocs,
                   includeTotalGroupCount: Boolean = true): Task[LuceneGroupedSearchResults[Doc, Model, G, V]] = query.transaction match {
      case tx: LuceneTransaction[Doc, Model] =>
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
        Task.error(new RuntimeException("Lucene grouping is only supported when using a Lucene-backed collection"))
    }
  }
}

