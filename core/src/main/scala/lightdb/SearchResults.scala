package lightdb

import lightdb.doc.{Document, DocumentModel}
import lightdb.facet.FacetResult
import lightdb.field.Field._
import lightdb.transaction.Transaction
import rapid.Task

case class SearchResults[Doc <: Document[Doc], Model <: DocumentModel[Doc], V](model: Model,
                                                                               offset: Int,
                                                                               limit: Option[Int],
                                                                               total: Option[Int],
                                                                               streamWithScore: rapid.Stream[(V, Double)],
                                                                               facetResults: Map[FacetField[Doc], FacetResult],
                                                                               transaction: Transaction[Doc]) {
  def stream: rapid.Stream[V] = streamWithScore.map(_._1)

  lazy val listWithScore: Task[List[(V, Double)]] = streamWithScore.toList
  lazy val list: Task[List[V]] = listWithScore.map(_.map(_._1))
  lazy val scores: Task[List[Double]] = listWithScore.map(_.map(_._2))
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
