package lightdb.index

import fabric.Json
import lightdb.document.{Document, DocumentModel}

case class MaterializedIndex[D <: Document[D], M <: DocumentModel[D]](json: Json, model: M) extends Materialized[D] {
  def get[F](f: M => Index[F, D]): Option[F] = {
    val index = f(model)
    get(index.name, index.rw)
  }

  def apply[F](f: M => Index[F, D]): F = {
    val index = f(model)
    apply(index.name, index.rw)
  }
}
