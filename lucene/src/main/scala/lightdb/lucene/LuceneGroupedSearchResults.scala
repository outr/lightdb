package lightdb.lucene

import lightdb.doc.{Document, DocumentModel}
import lightdb.transaction.Transaction
import rapid.Grouped

case class LuceneGroupedResult[G, V](group: G, results: List[(V, Double)]) {
  def withoutScores: Grouped[G, V] = Grouped(group, results.map(_._1))
}

case class LuceneGroupedSearchResults[Doc <: Document[Doc], Model <: DocumentModel[Doc], G, V](model: Model,
                                                                                               offset: Int,
                                                                                               limit: Option[Int],
                                                                                               totalGroups: Option[Int],
                                                                                               groups: List[LuceneGroupedResult[G, V]],
                                                                                               transaction: Transaction[Doc, Model]) {
  def stream: rapid.Stream[LuceneGroupedResult[G, V]] =
    rapid.Stream.fromIterator(rapid.Task(groups.iterator))

  def grouped: List[Grouped[G, V]] = groups.map(_.withoutScores)
}

