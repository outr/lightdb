package lightdb.model

import fabric.rw.RW
import lightdb.{CommitMode, Document, LightDB}

abstract class Collection[D <: Document[D]](val collectionName: String,
                                            val db: LightDB,
                                            commitMode: CommitMode = CommitMode.Manual,
                                            val atomic: Boolean = true) extends AbstractCollection[D] with DocumentModel[D] {
  override def model: DocumentModel[D] = this

  override def defaultCommitMode: CommitMode = commitMode
}

object Collection {
  def apply[D <: Document[D]](collectionName: String,
                              db: LightDB,
                              defaultCommitMode: CommitMode = CommitMode.Manual,
                              atomic: Boolean = true)(implicit docRW: RW[D]): Collection[D] =
    new Collection[D](collectionName, db, defaultCommitMode, atomic) {
      override implicit val rw: RW[D] = docRW
    }
}