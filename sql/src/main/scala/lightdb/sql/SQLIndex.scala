package lightdb.sql

import fabric.rw.{Convertible, RW}
import lightdb.index.{Index, IndexSupport}
import lightdb.Document
import lightdb.query.Filter

case class SQLIndex[F, D <: Document[D]](fieldName: String,
                                         indexSupport: IndexSupport[D],
                                         get: D => List[F])(implicit val rw: RW[F]) extends Index[F, D] {
  override def is(value: F): Filter[D] = SQLFilter[D](s"$fieldName = ?", List(value.json))

  override def >(value: F)(implicit num: Numeric[F]): Filter[D] = SQLFilter[D](s"$fieldName > ?", List(value.json))
  override def >=(value: F)(implicit num: Numeric[F]): Filter[D] = SQLFilter[D](s"$fieldName >= ?", List(value.json))
  override def <(value: F)(implicit num: Numeric[F]): Filter[D] = SQLFilter[D](s"$fieldName < ?", List(value.json))
  override def <=(value: F)(implicit num: Numeric[F]): Filter[D] = SQLFilter[D](s"$fieldName <= ?", List(value.json))

  override protected def rangeLong(from: Long, to: Long): Filter[D] = SQLFilter[D](s"$fieldName BETWEEN ? AND ?", List(from.json, to.json))

  override protected def rangeDouble(from: Double, to: Double): Filter[D] = SQLFilter[D](s"$fieldName BETWEEN ? AND ?", List(from.json, to.json))

  override def IN(values: Seq[F]): Filter[D] = SQLFilter[D](s"$fieldName IN (${values.map(_ => "?").mkString(", ")})", values.toList.map(_.json))
}
