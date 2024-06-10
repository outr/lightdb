package lightdb.sql

import fabric.Json
import fabric.rw._
import lightdb.Document
import lightdb.index.FilterSupport

trait SQLFilterSupport[F, D <: Document[D], Filter] extends FilterSupport[F, D, Filter] {
  protected def createFilter(sql: String, args: List[Json]): Filter

  def fieldName: String

  override def is(value: F): Filter = createFilter(s"$fieldName = ?", List(value.json))

  override def >(value: F)(implicit num: Numeric[F]): Filter = createFilter(s"$fieldName > ?", List(value.json))
  override def >=(value: F)(implicit num: Numeric[F]): Filter = createFilter(s"$fieldName >= ?", List(value.json))
  override def <(value: F)(implicit num: Numeric[F]): Filter = createFilter(s"$fieldName < ?", List(value.json))
  override def <=(value: F)(implicit num: Numeric[F]): Filter = createFilter(s"$fieldName <= ?", List(value.json))

  override def rangeLong(from: Long, to: Long): Filter = createFilter(s"$fieldName BETWEEN ? AND ?", List(from.json, to.json))

  override def rangeDouble(from: Double, to: Double): Filter = createFilter(s"$fieldName BETWEEN ? AND ?", List(from.json, to.json))

  override def IN(values: Seq[F]): Filter = createFilter(s"$fieldName IN (${values.map(_ => "?").mkString(", ")})", values.toList.map(_.json))
}
