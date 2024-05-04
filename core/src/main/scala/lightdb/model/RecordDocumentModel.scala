package lightdb.model

import cats.effect.IO
import fabric._
import lightdb.RecordDocument

trait RecordDocumentModel[D <: RecordDocument[D]] extends DocumentModel[D] {
  override def preSetJson(json: Json, collection: AbstractCollection[D]): IO[Json] = IO {
    json.modify("modified") { _ =>
      System.currentTimeMillis()
    }
  }
}
