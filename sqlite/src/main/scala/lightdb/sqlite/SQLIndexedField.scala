package lightdb.sqlite

import lightdb.index.IndexedField
import lightdb.query.Filter
import lightdb.Document
import lightdb.model.Collection

case class SQLIndexedField[F, D <: Document[D]](fieldName: String,
                                                collection: Collection[D],
                                                get: D => Option[F]) extends IndexedField[F, D] {
  def ===(value: F): Filter[D] = is(value)

  def is(value: F): Filter[D] = SQLFilter[D](s"$fieldName = ?", List(value))

  def between(v1: F, v2: F): Filter[D] = SQLFilter[D](s"$fieldName BETWEEN ? AND ?", List(v1, v2))

  def IN(values: Seq[F]): Filter[D] = SQLFilter[D](s"$fieldName IN (${values.map(_ => "?").mkString(", ")})", values.toList)
}
