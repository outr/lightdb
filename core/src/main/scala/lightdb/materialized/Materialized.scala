package lightdb.materialized

import fabric.Json
import fabric.rw._
import lightdb.doc.{Document, DocumentModel}

trait Materialized[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
  def json: Json
  def model: Model

  protected def get[F](name: String, rw: RW[F]): Option[F] = try {
    json.get(name).map(_.as[F](rw))
  } catch {
    case t: Throwable => throw new RuntimeException(s"Failed to materialize $name, JSON: $json", t)
  }

  protected def apply[F](name: String, rw: RW[F]): F = get[F](name, rw)
    .getOrElse(throw new NullPointerException(s"$name not found in $json"))

  def as[T](implicit rw: RW[T]): T = json.as[T]
}
