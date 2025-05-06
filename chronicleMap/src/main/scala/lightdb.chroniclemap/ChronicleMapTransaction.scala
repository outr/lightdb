package lightdb.chroniclemap

import fabric.Json
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.transaction.Transaction
import net.openhft.chronicle.map.ChronicleMap
import rapid.Task

import scala.jdk.CollectionConverters.IteratorHasAsScala

case class ChronicleMapTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: ChronicleMapStore[Doc, Model],
                                                                                      db: ChronicleMap[String, String],
                                                                                      parent: Option[Transaction[Doc, Model]]) extends Transaction[Doc, Model] {
  override def jsonStream: rapid.Stream[Json] = rapid.Stream.fromIterator(Task {
    db.values()
      .iterator()
      .asScala
      .map(toJson)
  })

  override protected def _get[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = Task {
    if (index == store.idField) {
      Option(db.get(value.asInstanceOf[Id[Doc]].value)).map(fromString)
    } else {
      throw new UnsupportedOperationException(s"ChronicleMapStore can only get on _id, but ${index.name} was attempted")
    }
  }

  override protected def _insert(doc: Doc): Task[Doc] = Task {
    if (db.putIfAbsent(doc._id.value, toString(doc)) != null) {
      throw new RuntimeException(s"${doc._id.value} already exists")
    }
    doc
  }

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    db.put(doc._id.value, toString(doc))
    doc
  }

  override protected def _exists(id: Id[Doc]): Task[Boolean] = Task {
    db.containsKey(id.value)
  }

  override protected def _count: Task[Int] = Task(db.size())

  override protected def _delete[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Boolean] = Task {
    if (index == store.idField) {
      db.remove(value.asInstanceOf[Id[Doc]].value) != null
    } else {
      throw new UnsupportedOperationException(s"ChronicleMapStore can only get on _id, but ${index.name} was attempted")
    }
  }

  override protected def _commit: Task[Unit] = Task.unit

  override protected def _rollback: Task[Unit] = Task.unit

  override protected def _close: Task[Unit] = Task.unit

  override def truncate: Task[Int] = count.map { size =>
    db.clear()
    size
  }
}
