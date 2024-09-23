package lightdb.async

import cats.effect.IO
import lightdb.doc.{Document, DocumentModel}
import lightdb.facet.FacetResult
import lightdb.field.Field.FacetField
import lightdb.transaction.Transaction

case class AsyncSearchResults[Doc <: Document[Doc], Model <: DocumentModel[Doc], V](model: Model,
                                                                                    offset: Int,
                                                                                    limit: Option[Int],
                                                                                    total: Option[Int],
                                                                                    scoredStream: fs2.Stream[IO, (V, Double)],
                                                                                    facetResults: Map[FacetField[Doc], FacetResult],
                                                                                    transaction: Transaction[Doc]) {
  def stream: fs2.Stream[IO, V] = scoredStream.map(_._1)

  def first: IO[Option[V]] = stream.take(1).compile.toList.map(_.headOption)

  def one: IO[V] = first.map {
    case Some(v) => v
    case None => throw new NullPointerException("No results for search")
  }

  def facet(f: Model => FacetField[Doc]): FacetResult = facetResults(f(model))

  def getFacet(f: Model => FacetField[Doc]): Option[FacetResult] = facetResults.get(f(model))
}