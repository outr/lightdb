package lightdb.tantivy

import lightdb.{Sort, SortDirection}
import scantivy.proto as pb

object TantivySort {

  def compile(sort: Sort): pb.SortClause = sort match {
    case b: Sort.BestMatch =>
      pb.SortClause(pb.SortClause.Clause.Relevance(pb.SortRelevance(direction = direction(b.direction))))
    case Sort.IndexOrder =>
      pb.SortClause(pb.SortClause.Clause.IndexOrder(pb.SortIndexOrder(direction = pb.SortDirection.SORT_ASC)))
    case bf: Sort.ByField[?, ?] =>
      pb.SortClause(pb.SortClause.Clause.ByField(pb.SortByField(field = bf.field.name, direction = direction(bf.direction))))
    case _: Sort.ByDistance[?, ?] =>
      throw new UnsupportedOperationException(
        "Tantivy backend does not support Sort.ByDistance — no native geo support. " +
          "Use a backend like lucene; this is deliberately not emulated."
      )
  }

  private def direction(d: SortDirection): pb.SortDirection = d match {
    case SortDirection.Ascending => pb.SortDirection.SORT_ASC
    case SortDirection.Descending => pb.SortDirection.SORT_DESC
  }
}
