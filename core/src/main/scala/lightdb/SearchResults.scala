package lightdb

import lightdb.doc.Document
import lightdb.transaction.Transaction

case class SearchResults[Doc <: Document[Doc], V](offset: Int,
                                 limit: Option[Int],
                                 total: Option[Int],
                                 iteratorWithScore: Iterator[(V, Double)],
                                 transaction: Transaction[Doc]) {
  def iterator: Iterator[V] = iteratorWithScore.map(_._1)
  lazy val list: List[V] = iterator.toList
}
