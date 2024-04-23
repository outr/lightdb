package lightdb

import cats.effect.IO
import fabric._

abstract class RecordDocumentCollection[D <: RecordDocument[D]](collectionName: String, db: LightDB) extends Collection[D](collectionName, db) {
  override protected def preSetJson(json: Json): IO[Json] = IO {
    json.modify("modified") { _ =>
      System.currentTimeMillis()
    }
  }
}