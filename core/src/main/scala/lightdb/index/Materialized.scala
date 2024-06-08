package lightdb.index

import lightdb.{Document, Id}

class Materialized[D <: Document[D]](map: Map[Index[_, D], Any]) {
  def get[F](index: Index[F, D]): Option[F] = map.get(index).map(_.asInstanceOf[F])
  def apply[F](index: Index[F, D]): F = get(index).getOrElse(throw new NullPointerException(s"${index.fieldName} not found in [${map.keySet.map(_.fieldName).mkString(", ")}]"))
}