package lightdb.model

import cats.effect.IO
import fabric._
import lightdb.RecordDocument

trait RecordDocumentModel[D <: RecordDocument[D]] extends DocumentModel[D] {
  override protected[lightdb] def initModel(collection: AbstractCollection[D]): Unit = {
    super.initModel(collection)
    collection.preSetJson.add((_: DocumentAction, json: Json, _: AbstractCollection[D]) => IO {
      Some(json.modify("modified")(_ => System.currentTimeMillis()))
    })
  }
}
