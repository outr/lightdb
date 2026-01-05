package lightdb.filter

import lightdb.doc.Document

object QueryOptimizer {
  /**
   * Recursively optimize a filter by flattening unnecessary nested Multi filters.
   */
  def optimize[Doc <: Document[Doc]](filter: Filter[Doc]): Filter[Doc] = filter match {
    case multi: Filter.Multi[Doc] =>
      // Recursively optimize inner filters for every clause.
      val optimizedClauses: List[FilterClause[Doc]] = multi.filters.map { clause =>
        clause.copy(filter = optimize(clause.filter))
      }
      // Flatten nested Multi clauses if the conditions allow.
      val flattenedClauses: List[FilterClause[Doc]] = optimizedClauses.flatMap {
        case FilterClause(innerMulti: Filter.Multi[Doc] @unchecked, cond, boost)
          if boost.isEmpty && canMerge(cond) =>
          innerMulti.filters
        case clause =>
          List(clause)
      }
      multi.copy(filters = flattenedClauses)
    case other =>
      other
  }

  /**
   * Determines whether the condition type allows for merging nested multi filters.
   * For example, Must and Should conditions can be merged.
   */
  private def canMerge(condition: Condition): Boolean =
    condition == Condition.Must || condition == Condition.Filter || condition == Condition.Should
}