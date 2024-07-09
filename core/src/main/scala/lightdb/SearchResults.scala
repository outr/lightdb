package lightdb

import lightdb.transaction.Transaction

case class SearchResults[Doc, V](offset: Int,
                                 limit: Option[Int],
                                 total: Option[Int],
                                 iterator: Iterator[V],
                                 transaction: Transaction[Doc]) {
  lazy val list: List[V] = iterator.toList
}
