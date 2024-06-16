package lightdb.collection

import cats.effect.IO
import lightdb.LightDB
import lightdb.document.{Document, DocumentModel}
import lightdb.util.Initializable

case class Collection[D <: Document[D]](name: String,
                                        model: DocumentModel[D],
                                        db: LightDB) extends Initializable {
  override protected def initialize(): IO[Unit] = model.init(this)
}