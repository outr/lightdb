package lightdb

import cats.effect.IO

abstract class RecordDocumentCollection[D <: RecordDocument[D]](collectionName: String, db: LightDB) extends Collection[D](collectionName, db) {
  override protected def preSet(doc: D): IO[D] = super.preSet(doc.modify())
}
