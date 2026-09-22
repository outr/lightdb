package lightdb.indexeddb

import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id
import lightdb.transaction.Transaction
import rapid.Task

import scala.collection.mutable
import scala.scalajs.js

/**
 * A LightDB transaction over an [[IndexedDBStore]].
 *
 * IndexedDB transactions cannot stay open across asynchronous work (see [[IDB]]), so this transaction never holds one.
 * Writes are buffered here and applied at commit in a single IndexedDB read-write transaction, which makes the commit
 * atomic. Reads see the buffer first, so a transaction reads its own writes. Rollback discards the buffer; nothing was
 * written to IndexedDB, so rollback is exact rather than best effort.
 */
case class IndexedDBTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: IndexedDBStore[Doc, Model],
  parent: Option[Transaction[Doc, Model]],
  writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]
) extends Transaction[Doc, Model] {
  override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)

  // Pending writes in order, by id: Some(json) is an upsert, None is a delete.
  private val pending = mutable.LinkedHashMap.empty[String, Option[String]]
  // True once truncate is called: committed documents are hidden and the object store is cleared at commit.
  private var truncated = false

  private def encode(doc: Doc): String = JsonFormatter.Compact(doc.json(store.model.rw))
  private def decode(json: String): Doc = JsonParser(json).as[Doc](store.model.rw)

  private def committed(id: String): Task[Option[String]] =
    if (truncated) Task.pure(None)
    else IDB.read(store.db)(_.get(id)).map(v => if (js.isUndefined(v)) None else Some(v.asInstanceOf[String]))

  private def lookup(id: String): Task[Option[String]] = pending.get(id) match {
    case Some(state) => Task.pure(state)
    case None => committed(id)
  }

  /** Committed documents overlaid with this transaction's pending writes. */
  private def merged: Task[List[String]] = {
    val base: Task[List[String]] =
      if (truncated) Task.pure(Nil)
      else IDB.read(store.db)(_.getAll()).map(_.toList.map(_.asInstanceOf[String]))
    base.map { jsons =>
      val byId = mutable.LinkedHashMap.from(jsons.map(j => decode(j)._id.value -> j))
      pending.foreach {
        case (id, Some(json)) => byId.update(id, json)
        case (id, None) => byId.remove(id)
      }
      byId.values.toList
    }
  }

  override def jsonStream: rapid.Stream[Json] = rapid.Stream.force(merged.map(list => rapid.Stream.emits(list.map(JsonParser(_)))))

  override protected def _get[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]] =
    if (index == store.idField) lookup(value.asInstanceOf[Id[Doc]].value).map(_.map(decode))
    else Task.error(new UnsupportedOperationException(s"IndexedDBStore can only get on _id, but ${index.name} was attempted"))

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    pending.update(doc._id.value, Some(encode(doc)))
    doc
  }

  override protected def _exists(id: Id[Doc]): Task[Boolean] = lookup(id.value).map(_.nonEmpty)

  override protected def _count: Task[Int] = merged.map(_.size)

  override def _delete(id: Id[Doc]): Task[Boolean] = _exists(id).map { existed =>
    pending.update(id.value, None)
    existed
  }

  override def truncate: Task[Int] = _count.map { count =>
    pending.clear()
    truncated = true
    count
  }

  override protected def _commit: Task[Unit] = Task.defer {
    if (pending.isEmpty && !truncated) Task.unit
    else {
      val clear = truncated
      val writes = pending.toList
      IDB.write(store.db) { objectStore =>
        if (clear) objectStore.clear()
        writes.foreach {
          case (id, Some(json)) => objectStore.put(json, id)
          case (id, None) => objectStore.delete(id)
        }
      }.map { _ =>
        pending.clear()
        truncated = false
      }
    }
  }

  override protected def _rollback: Task[Unit] = Task {
    pending.clear()
    truncated = false
  }

  override protected def _close: Task[Unit] = Task.unit
}
