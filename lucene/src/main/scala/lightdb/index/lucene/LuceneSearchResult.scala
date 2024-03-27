package lightdb.index.lucene

import cats.effect.IO
import lightdb.collection.Collection
import lightdb.field.Field
import lightdb.index.SearchResult
import lightdb.query.Query
import lightdb.{Document, Id}
import org.apache.lucene.document.StoredValue
import org.apache.lucene.index.StoredFields
import org.apache.lucene.search.ScoreDoc

case class LuceneSearchResult[D <: Document[D]](scoreDoc: ScoreDoc,
                                                collection: Collection[D],
                                                storedFields: StoredFields) extends SearchResult[D] {
  private lazy val document = storedFields.document(scoreDoc.doc)
  private lazy val doc = collection(id)

  lazy val id: Id[D] = Id[D](document.get("_id"))

  override def get(): IO[D] = doc

  override def apply[F](field: Field[D, F]): F = {
    val value = document.getField(field.name).storedValue()
    val f = value.getType match {
      case StoredValue.Type.STRING => value.getStringValue
      case StoredValue.Type.INTEGER => value.getIntValue
      case _ => throw new RuntimeException(s"Unsupported get: ${document.get(field.name)} (${field.name} / ${value.getType})")
    }
    f.asInstanceOf[F]
  }
}
