package lightdb.index

import fabric.Json
import fabric.rw.{Asable, RW}
import lightdb.{Document, Id}

case class Materialized[D <: Document[D]](json: Json) {
  def get[F](index: Index[F, D]): Option[F] = json.get(index.fieldName).map(_.as[F](index.rw))
  def apply[F](index: Index[F, D]): F = get(index).getOrElse(throw new NullPointerException(s"${index.fieldName} not found in $json"))
  def as[T](implicit rw: RW[T]): T = json.as[T]
}