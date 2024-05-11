package lightdb.model

import fabric.rw.RW
import lightdb.{Document, LightDB}

abstract class Collection[D <: Document[D]](val collectionName: String,
                                            protected[lightdb] val db: LightDB,
                                            val autoCommit: Boolean = false,
                                            val atomic: Boolean = true) extends AbstractCollection[D] with DocumentModel[D] {
  override def model: DocumentModel[D] = this
}

object Collection {
  def apply[D <: Document[D]](collectionName: String,
                              db: LightDB,
                              autoCommit: Boolean = false,
                              atomic: Boolean = true)(implicit docRW: RW[D]): Collection[D] =
    new Collection[D](collectionName, db, autoCommit = autoCommit, atomic = atomic) {
      override implicit val rw: RW[D] = docRW
    }
}