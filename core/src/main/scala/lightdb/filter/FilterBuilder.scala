package lightdb.filter

import lightdb.doc.{Document, DocumentModel}

class FilterBuilder[Doc <: Document[Doc], Model <: DocumentModel[Doc]](val model: Model,
                                                                       minShould: Int,
                                                                       filters: List[FilterClause[Doc]]) extends Filter.Multi[Doc](minShould, filters) {
  def minShould(i: Int): FilterBuilder[Doc, Model] = new FilterBuilder(model, i, filters)

  def withFilter(filter: Filter[Doc], condition: Condition, boost: Option[Double] = None): FilterBuilder[Doc, Model] =
    new FilterBuilder(model, minShould, filters = filters ::: List(FilterClause(filter, condition, boost)))

  def must(f: Model => Filter[Doc], boost: Option[Double] = None): FilterBuilder[Doc, Model] = withFilter(f(model), Condition.Must, boost)
  def mustNot(f: Model => Filter[Doc], boost: Option[Double] = None): FilterBuilder[Doc, Model] = withFilter(f(model), Condition.MustNot, boost)
  def filter(f: Model => Filter[Doc], boost: Option[Double] = None): FilterBuilder[Doc, Model] = withFilter(f(model), Condition.Filter, boost)
  def should(f: Model => Filter[Doc], boost: Option[Double] = None): FilterBuilder[Doc, Model] = withFilter(f(model), Condition.Should, boost)
}
