package lightdb

import cats.effect.IO
import fabric._
import lightdb.model.{AbstractCollection, Collection}

abstract class RecordDocumentCollection[D <: RecordDocument[D]](collectionName: String, db: LightDB) extends Collection[D](collectionName, db) {
  override def preSetJson(json: Json, collection: AbstractCollection[D]): IO[Json] = IO {
    json.modify("modified") { _ =>
      System.currentTimeMillis()
    }
  }
}