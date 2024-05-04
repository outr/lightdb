package lightdb.sqlite

import fabric.rw.{Convertible, RW}
import lightdb.index.IndexedField
import lightdb.query.Filter
import lightdb.Document
import lightdb.model.Collection

case class SQLIndexedField[F, D <: Document[D]](fieldName: String,
                                                collection: Collection[D],
                                                get: D => Option[F])(implicit val rw: RW[F]) extends IndexedField[F, D] {
  def ===(value: F): SQLFilter[D] = is(value)

  def is(value: F): SQLFilter[D] = SQLFilter[D](s"$fieldName = ?", List(value.json))

  def between(v1: F, v2: F): SQLFilter[D] = SQLFilter[D](s"$fieldName BETWEEN ? AND ?", List(v1.json, v2.json))

  def IN(values: Seq[F]): SQLFilter[D] = SQLFilter[D](s"$fieldName IN (${values.map(_ => "?").mkString(", ")})", values.toList.map(_.json))
}
