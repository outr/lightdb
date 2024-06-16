package lightdb.index

import fabric.Json
import fabric.rw.{Convertible, RW}
import lightdb.aggregate.AggregateFilter
import lightdb.document.Document
import lightdb.filter.{Filter, FilterSupport}

trait Index[F, D <: Document[D]] extends FilterSupport[F, D, Filter[D]] {
  def name: String
  def indexer: Indexer[D]
  def get: D => List[F]
  def getJson: D => List[Json] = (doc: D) => get(doc).map(_.json)
  def aggregate(name: String): FilterSupport[F, D, AggregateFilter[D]]
}