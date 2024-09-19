package lightdb

import lightdb.Field._
import lightdb.doc.{Document, DocumentModel}
import lightdb.facet.FacetResult
import lightdb.transaction.Transaction

case class SearchResults[Doc <: Document[Doc], Model <: DocumentModel[Doc], V](model: Model,
                                                                               offset: Int,
                                                                               limit: Option[Int],
                                                                               total: Option[Int],
                                                                               iteratorWithScore: Iterator[(V, Double)],
                                                                               facetResults: Map[FacetField[Doc], FacetResult],
                                                                               transaction: Transaction[Doc]) {
  def iterator: Iterator[V] = iteratorWithScore.map(_._1)

  lazy val listWithScore: List[(V, Double)] = iteratorWithScore.toList
  lazy val list: List[V] = listWithScore.map(_._1)
  lazy val scores: List[Double] = listWithScore.map(_._2)
  /**
   * Represents the total minus the current offset.
   *
   * Note: This represents the remaining when this query was executed because the number of records returned in this
   * result set is unknown.
   */
  lazy val remaining: Option[Int] = total.map(t => t - offset)

  def facet(f: Model => FacetField[Doc]): FacetResult = facetResults(f(model))

  def getFacet(f: Model => FacetField[Doc]): Option[FacetResult] = facetResults.get(f(model))
}
