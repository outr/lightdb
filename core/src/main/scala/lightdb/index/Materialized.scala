package lightdb.index

import fabric.Json
import fabric.rw.{Asable, RW}
import lightdb.aggregate.AggregateFunction
import lightdb.{Document, Id}

case class Materialized[D <: Document[D]](json: Json) {
  private def get[F](name: String, rw: RW[F]): Option[F] = try {
    json.get(name).map(_.as[F](rw))
  } catch {
    case t: Throwable => throw new RuntimeException(s"Failed to materialize $name, JSON: $json", t)
  }
  def get[F](index: Index[F, D]): Option[F] = get(index.fieldName, index.rw)
  def get[T, F](function: AggregateFunction[T, F, D]): Option[T] = get(function.name, function.rw)

  def apply[F](index: Index[F, D]): F = get(index).getOrElse(throw new NullPointerException(s"${index.fieldName} not found in $json"))
  def apply[T, F](function: AggregateFunction[T, F, D]): T = get(function).getOrElse(throw new NullPointerException(s"${function.name} not found in $json"))

  def as[T](implicit rw: RW[T]): T = json.as[T]
}