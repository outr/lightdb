package lightdb.sqlite

import cats.effect.IO
import lightdb.{Document, Id}
import lightdb.index.Indexer
import lightdb.query.SearchContext

case class SQLiteIndexer[D <: Document[D]](indexSupport: SQLiteSupport[D]) extends Indexer[D] {
  override def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return] = {
    val context = SearchContext(indexSupport)
    f(context)
  }

  def apply[F](name: String, get: D => Option[F]): SQLIndexedField[F, D] = SQLIndexedField(
    fieldName = name,
    collection = indexSupport,
    get = get
  )

  override def count(): IO[Int] = IO {
    val ps = indexSupport.connection.prepareStatement(s"SELECT COUNT(_id) FROM ${indexSupport.collectionName}")
    try {
      val rs = ps.executeQuery()
      rs.next()
      rs.getInt(1)
    } finally {
      ps.close()
    }
  }

  override private[lightdb] def delete(id: Id[D]): IO[Unit] = IO {
    val ps = indexSupport.connection.prepareStatement(s"DELETE FROM ${indexSupport.collectionName} WHERE _id = ?")
    try {
      ps.setString(1, id.value)
      ps.executeUpdate()
    } finally {
      ps.close()
    }
  }

  override def commit(): IO[Unit] = IO.unit
}