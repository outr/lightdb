package lightdb.model

import cats.effect.IO
import lightdb.Document

case class DocumentActions[D <: Document[D], Value](action: DocumentAction) {
  private var list = List.empty[DocumentListener[D, Value]]

  def +=(listener: DocumentListener[D, Value]): Unit = add(listener)

  def add(listener: DocumentListener[D, Value]): Unit = synchronized {
    list = list ::: List(listener)
  }

  private[model] def invoke(value: Value, collection: AbstractCollection[D]): IO[Option[Value]] = {
    def recurse(value: Value, list: List[DocumentListener[D, Value]]): IO[Option[Value]] = if (list.isEmpty) {
      IO.pure(Some(value))
    } else {
      list.head(action, value, collection).flatMap {
        case Some(v) => recurse(v, list.tail)
        case None => IO.pure(None)
      }
    }

    recurse(value, list)
  }
}
