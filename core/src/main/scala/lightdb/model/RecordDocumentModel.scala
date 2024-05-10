package lightdb.model

import cats.effect.IO
import fabric._
import lightdb.RecordDocument

trait RecordDocumentModel[D <: RecordDocument[D]] extends DocumentModel[D] {
  override protected[lightdb] def initModel(collection: AbstractCollection[D]): Unit = {
    super.initModel(collection)
    collection.preSetJson.add(new DocumentListener[D, Json] {
      override def apply(action: DocumentAction, json: Json, collection: AbstractCollection[D]): IO[Option[Json]] = IO {
        Some(json.modify("modified")(_ => System.currentTimeMillis()))
      }
    })
  }
}
