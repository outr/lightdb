package lightdb.index.lucene

import cats.effect.IO
import com.outr.lucene4s.query.SearchResult
import lightdb.field.Field
import lightdb.query.ResultDoc
import lightdb.{Document, Id}

case class LuceneResultDoc[D <: Document[D]](results: LucenePagedResults[D], result: SearchResult) extends ResultDoc[D] {
  override lazy val id: Id[D] = result(results.indexer.id.luceneField)

  override def get(): IO[D] = results.query.collection(id)

  override def apply[F](field: Field[D, F]): F = {
    val indexedField = results.indexer.fields.find(_.field.name == field.name).getOrElse(throw new RuntimeException(s"Unable to find indexed field for: ${field.name}"))
    result(indexedField.luceneField).asInstanceOf[F]
  }
}