package lightdb

import lightdb.doc.Document
import lightdb.transaction.Transaction

case class SearchResults[Doc <: Document[Doc], V](offset: Int,
                                 limit: Option[Int],
                                 total: Option[Int],
                                 iteratorWithScore: Iterator[(V, Double)],
                                 transaction: Transaction[Doc]) {
  def iterator: Iterator[V] = iteratorWithScore.map(_._1)
  lazy val listWithScore: List[(V, Double)] = iteratorWithScore.toList
  lazy val list: List[V] = listWithScore.map(_._1)
  lazy val scores: List[Double] = listWithScore.map(_._2)
}
