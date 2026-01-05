package lightdb.opensearch

import lightdb.doc.{Document, DocumentModel}
import lightdb.transaction.Transaction
import rapid.Grouped

case class OpenSearchGroupedResult[G, V](group: G, results: List[(V, Double)]) {
  def withoutScores: Grouped[G, V] = Grouped(group, results.map(_._1))
}

case class OpenSearchGroupedSearchResults[Doc <: Document[Doc], Model <: DocumentModel[Doc], G, V](model: Model,
                                                                                                  offset: Int,
                                                                                                  limit: Option[Int],
                                                                                                  totalGroups: Option[Int],
                                                                                                  groups: List[OpenSearchGroupedResult[G, V]],
                                                                                                  transaction: Transaction[Doc, Model]) {
  def stream: rapid.Stream[OpenSearchGroupedResult[G, V]] =
    rapid.Stream.fromIterator(rapid.Task(groups.iterator))

  def grouped: List[Grouped[G, V]] = groups.map(_.withoutScores)
}



