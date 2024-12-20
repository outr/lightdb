package lightdb.async

import lightdb.doc.{Document, DocumentModel}
import lightdb.facet.FacetResult
import lightdb.field.Field.FacetField
import lightdb.transaction.Transaction
import rapid.Task

case class AsyncSearchResults[Doc <: Document[Doc], Model <: DocumentModel[Doc], V](model: Model,
                                                                                    offset: Int,
                                                                                    limit: Option[Int],
                                                                                    total: Option[Int],
                                                                                    scoredStream: rapid.Stream[(V, Double)],
                                                                                    facetResults: Map[FacetField[Doc], FacetResult],
                                                                                    transaction: Transaction[Doc]) {
  def stream: rapid.Stream[V] = scoredStream.map(_._1)

  def first: Task[Option[V]] = stream.take(1).lastOption

  def one: Task[V] = first.map {
    case Some(v) => v
    case None => throw new NullPointerException("No results for search")
  }

  def facet(f: Model => FacetField[Doc]): FacetResult = facetResults(f(model))

  def getFacet(f: Model => FacetField[Doc]): Option[FacetResult] = facetResults.get(f(model))
}