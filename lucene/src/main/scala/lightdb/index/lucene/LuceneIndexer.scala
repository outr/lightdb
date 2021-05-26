package lightdb.index.lucene

import cats.effect.IO
import com.outr.lucene4s._
import com.outr.lucene4s.field.value.FieldAndValue
import com.outr.lucene4s.field.{Field => LuceneField}
import com.outr.lucene4s.query.{MatchAllSearchTerm, Sort => LuceneSort}
import lightdb.collection.Collection
import lightdb.field.Field
import lightdb.index.Indexer
import lightdb.query.{Filter, PagedResults, Query, Sort}
import lightdb.{Document, Id}
import org.apache.lucene.queryparser.classic.QueryParserBase

case class LuceneIndexer[D <: Document[D]](collection: Collection[D], autoCommit: Boolean = false) extends Indexer[D] {
  private val lucene = new DirectLucene(
    uniqueFields = List("_id"),
    defaultFullTextSearchable = true,
    autoCommit = autoCommit
  )
  private[lucene] val _fields: List[IndexedField[Any]] = collection.mapping.fields.flatMap { field =>
    val indexFeatureOption = field.features.collectFirst {
      case indexFeature: IndexFeature[_] => indexFeature.asInstanceOf[IndexFeature[Any]]
    }
    indexFeatureOption.map(indexFeature => IndexedField[Any](indexFeature.createField(field.name, lucene), field.asInstanceOf[Field[D, Any]]))
  }

  private[lucene] val fields: List[IndexedField[Any]] = _fields match {
    case list if list.exists(_.field.name == "_id") => list
    case list => id.asInstanceOf[IndexedField[Any]] :: list
  }

  lazy val id: IndexedField[Id[D]] = _fields
    .find(_.field.name == "_id")
    .map(_.asInstanceOf[IndexedField[Id[D]]])
    .getOrElse(IndexedField(lucene.create.field[Id[D]]("_id"), Field("_id", _._id, Nil)))

  private def ids(id: Id[D]): String = QueryParserBase.escape(id.value)

  override def put(value: D): IO[D] = if (fields.nonEmpty) {
    IO {
      val fieldsAndValues = fields.map(_.fieldAndValue(value))
      lucene
        .doc()
        .update(parse(s"_id:${ids(value._id)}"))
        .fields(fieldsAndValues: _*)
        .index()
      value
    }
  } else {
    IO.pure(value)
  }

  override def delete(id: Id[D]): IO[Unit] = IO(lucene.delete(parse(s"_id:${ids(id)}")))

  override def commit(): IO[Unit] = IO {
    lucene.commit()
  }

  override def count(): IO[Long] = IO {
    lucene.count()
  }

  override def search(query: Query[D]): IO[PagedResults[D]] = IO {
    var q = lucene.query().offset(query.offset).limit(query.limit)

    // Configure filters
    def indexedField(field: Field[Any, Any]): IndexedField[Any] = _fields
      .find(_.field.name == field.name)
      .getOrElse(throw new RuntimeException(s"Field is not indexed: ${field.name}"))

    def fieldAndValue(field: Field[Any, Any], value: Any): FieldAndValue[Any] = indexedField(field).luceneField(value)

    query.filters.foreach {
      case Filter.Equals(field, value) => {
        val fv = fieldAndValue(field.asInstanceOf[Field[Any, Any]], value)
        q = q.filter(exact(fv))
      }
      case Filter.NotEquals(field, value) => {
        val fv = fieldAndValue(field.asInstanceOf[Field[Any, Any]], value)
        q = q.filter(none(exact(fv)))
      }
    }

    // Sort
    q = q.sort(query.sort.map {
      case Sort.BestMatch => LuceneSort.Score
      case Sort.IndexOrder => LuceneSort.IndexOrder
      case Sort.ByField(field, reverse) => LuceneSort(indexedField(field.asInstanceOf[Field[Any, Any]]).luceneField, reverse)
    }: _*)

    LucenePagedResults(this, query, q.search())
  }

  override def truncate(): IO[Unit] = IO(lucene.delete(MatchAllSearchTerm))

  override def dispose(): IO[Unit] = IO(lucene.dispose())

  case class IndexedField[F](luceneField: LuceneField[F], field: Field[D, F]) {
    def fieldAndValue(value: D): FieldAndValue[F] = luceneField(field.getter(value))
  }
}