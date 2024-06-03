package lightdb.sqlite

import cats.effect.IO
import fabric.rw.RW
import lightdb.{Document, Id}
import lightdb.index.Indexer
import lightdb.model.AbstractCollection
import lightdb.query.SearchContext

case class SQLiteIndexer[D <: Document[D]](indexSupport: SQLiteSupport[D]) extends Indexer[D] {
  private def collection: AbstractCollection[D] = indexSupport.collection

  override def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return] = {
    val context = SearchContext(indexSupport)
    f(context)
  }

  def apply[F](name: String, get: D => List[F])(implicit rw: RW[F]): SQLIndexedField[F, D] = SQLIndexedField(
    fieldName = name,
    indexSupport = indexSupport,
    get = doc => get(doc)
  )

  def one[F](name: String, get: D => F)(implicit rw: RW[F]): SQLIndexedField[F, D] =
    apply[F](name, doc => List(get(doc)))

  override def truncate(): IO[Unit] = indexSupport.truncate()

  override def size: IO[Int] = IO.blocking {
    val ps = indexSupport.connection.prepareStatement(s"SELECT COUNT(_id) FROM ${collection.collectionName}")
    try {
      val rs = ps.executeQuery()
      rs.next()
      rs.getInt(1)
    } finally {
      ps.close()
    }
  }

  override private[lightdb] def delete(id: Id[D]): IO[Unit] = IO.blocking {
    indexSupport.backlog.remove(id)
    val ps = indexSupport.connection.prepareStatement(s"DELETE FROM ${collection.collectionName} WHERE _id = ?")
    try {
      ps.setString(1, id.value)
      ps.executeUpdate()
    } finally {
      ps.close()
    }
  }

  override def commit(): IO[Unit] = IO.unit
}