package lightdb.store

import lightdb._
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field._
import lightdb.field.IndexingState
import lightdb.transaction.Transaction
import lightdb.util.InMemoryIndex
import rapid.Task

trait InMemoryIndexes[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Store[Doc, Model] {
  private lazy val indexMap: Map[Indexed[Doc, _], InMemoryIndex[Doc, _]] = Map(fields.collect {
    case i: Indexed[Doc, _] => (i, new InMemoryIndex(i, None))
  }: _*)
  private lazy val indexes = indexMap.values.toList

  // Populate indexes
  transaction { implicit transaction =>
    val state = new IndexingState
    stream.foreach { doc =>
      indexes.foreach(_.set(doc, state))
    }.drain
  }.sync()

  abstract override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = {
    super.insert(doc).map { doc =>
      val state = new IndexingState
      indexes.foreach(_.set(doc, state))
      doc
    }
  }

  abstract override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = {
    super.upsert(doc).map { doc =>
      val state = new IndexingState
      indexes.foreach(_.set(doc, state))
      doc
    }
  }

  abstract override def delete[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Boolean] = {
    super.delete(field, value)
    // TODO: Support
  }

  override def doSearch[V](query: Query[Doc, Model],
                           conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] = {

    ???
  }

  abstract override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = {
    super.truncate().foreach { _ =>
      indexes.foreach(_.clear())
    }
  }
}
