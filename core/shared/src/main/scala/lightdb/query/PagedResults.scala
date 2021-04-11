package lightdb.query

import lightdb.Document

trait PagedResults[D <: Document[D]] {
  def query: Query[D]

  def total: Long

  def documents: List[ResultDoc[D]]
}