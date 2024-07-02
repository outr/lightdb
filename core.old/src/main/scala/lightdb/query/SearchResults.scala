package lightdb.query

import lightdb.document.Document
import lightdb.transaction.Transaction

case class SearchResults[D <: Document[D], V](offset: Int,
                                              limit: Option[Int],
                                              total: Option[Int],
                                              scoredIterator: Iterator[(V, Double)],
                                              transaction: Transaction[D]) {
  lazy val iterator: Iterator[V] = scoredIterator.map(_._1)
  lazy val list: List[V] = iterator.toList
}