package lightdb.model

import cats.effect.IO
import fabric.Json
import lightdb.{DocLock, Document, Id}

trait DocumentActionSupport[D <: Document[D]] {
  val preSet: DocumentActions[D, D] = DocumentActions[D, D](DocumentAction.PreSet)
  val preSetJson: DocumentActions[D, Json] = DocumentActions[D, Json](DocumentAction.PreSet)
  val postSet: DocumentActions[D, D] = DocumentActions[D, D](DocumentAction.PostSet)

  val preDeleteId: DocumentActions[D, Id[D]] = DocumentActions[D, Id[D]](DocumentAction.PreDelete)
  val preDelete: DocumentActions[D, D] = DocumentActions[D, D](DocumentAction.PreDelete)
  val postDelete: DocumentActions[D, D] = DocumentActions[D, D](DocumentAction.PostDelete)

  val commitActions: UnitActions = new UnitActions
  val disposeActions: UnitActions = new UnitActions

  protected def doSet(doc: D,
                      collection: AbstractCollection[D],
                      set: (Id[D], Json) => IO[Unit])
                     (implicit lock: DocLock[D]): IO[D] = preSet.invoke(doc, collection).flatMap {
    case Some(doc) => preSetJson.invoke(collection.rw.read(doc), collection).flatMap {
      case Some(json) => set(doc._id, json).flatMap { _ =>
        postSet.invoke(doc, collection).map(_ => doc)
      }
      case None => IO.pure(doc)
    }
    case None => IO.pure(doc)
  }

  protected def doDelete(id: Id[D],
                         collection: AbstractCollection[D],
                         get: Id[D] => IO[D],
                         delete: Id[D] => IO[Unit])
                        (implicit lock: DocLock[D]): IO[Id[D]] = preDeleteId.invoke(id, collection).flatMap {
    case Some(id) => get(id).flatMap(doc => preDelete.invoke(doc, collection).flatMap {
      case Some(doc) => delete(doc._id).flatMap { _ =>
        postDelete.invoke(doc, collection).map(_ => doc._id)
      }
      case None => IO.pure(id)
    })
    case None => IO.pure(id)
  }
}
