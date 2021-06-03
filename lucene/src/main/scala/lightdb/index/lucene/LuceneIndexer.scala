package lightdb.index.lucene

import cats.effect.IO
import com.outr.lucene4s._
import com.outr.lucene4s.field.value.FieldAndValue
import com.outr.lucene4s.field.{FieldType, Field => LuceneField}
import com.outr.lucene4s.query.{MatchAllSearchTerm, SearchTerm, Sort => LuceneSort, SearchResult => L4SSearchResult}
import lightdb.collection.Collection
import lightdb.field.Field
import lightdb.index.{Indexer, SearchResult}
import lightdb.query.{Filter, Query, Sort}
import lightdb.{Document, Id}
import fs2.Stream

case class LuceneIndexer[D <: Document[D]](collection: Collection[D], autoCommit: Boolean = false) extends Indexer[D] { li =>
  private val lucene = new DirectLucene(
    uniqueFields = List("_id"),
    directory = collection.db.directory.map(_.resolve(collection.collectionName)),
    defaultFullTextSearchable = true,
    autoCommit = autoCommit
  )
  private var _fields: List[IndexedField[Any]] = Nil
  private var fieldsMap: Map[String, IndexedField[Any]] = Map.empty

  private[lucene] def fields: List[IndexedField[Any]] = _fields

  val _id: IndexedField[Id[D]] = {
    collection.mapping.field.get[Id[D]]("_id").getOrElse(throw new RuntimeException("_id field not specified")).indexed(fieldType = FieldType.Untokenized)
    field[Id[D]]("_id")
  }

  object field {
    def get[F](name: String): Option[IndexedField[F]] = fieldsMap
      .get(name)
      .map(_.asInstanceOf[IndexedField[F]])
      .orElse {
        collection.mapping.field.get[F](name).flatMap { f =>
          f
            .features
            .find(_.isInstanceOf[IndexFeature[_]])
            .map(_.asInstanceOf[IndexFeature[F]])
            .map { indexFeature =>
              val indexedField = IndexedField(indexFeature.createField(name, lucene), f)
              li.synchronized {
                val aif = indexedField.asInstanceOf[IndexedField[Any]]
                _fields = _fields ::: List(aif)
                fieldsMap += name -> aif
              }
              indexedField
            }
        }
      }
    def apply[F](name: String): IndexedField[F] = get[F](name).getOrElse(throw new RuntimeException(s"Field not defined: $name"))
  }

  override def put(value: D): IO[D] = IO {
    val fields = collection.mapping.fields.flatMap(f => field.get[Any](f.name))
    if (fields.tail.nonEmpty) {     // No need to index if _id is the only field
      val fieldsAndValues = fields.map(_.fieldAndValue(value))
      lucene
        .doc()
        .update(exact(_id.luceneField(value._id)))
        .fields(fieldsAndValues: _*)
        .index()
    }
    value
  }

  override def delete(id: Id[D]): IO[Unit] = IO(lucene.delete(exact(_id.luceneField(id))))

  override def commit(): IO[Unit] = IO {
    lucene.commit()
  }

  override def count(): IO[Long] = IO {
    lucene.count()
  }

  private[lucene] def indexed[F](luceneField: LuceneField[F], field: Field[D, F]): Unit = {
    IndexedField[F](luceneField, field)
  }

  private def filter2Lucene(filter: Filter): SearchTerm = {
    def fieldAndValue(field: Field[D, Any], value: Any): FieldAndValue[Any] = this.field[Any](field.name).luceneField(value)
    filter match {
      case Filter.Equals(field, value) =>
        val fv = fieldAndValue(field.asInstanceOf[Field[D, Any]], value)
        exact(fv)
      case Filter.NotEquals(field, value) =>
        val fv = fieldAndValue(field.asInstanceOf[Field[D, Any]], value)
        none(exact(fv))
      case Filter.Includes(field, values) =>
        val terms = values.map(v => filter2Lucene(Filter.Equals(field.asInstanceOf[Field[D, Any]], v)))
        any(terms: _*)
      case Filter.Excludes(field, values) =>
        val terms = values.map(v => filter2Lucene(Filter.Equals(field.asInstanceOf[Field[D, Any]], v)))
        none(terms: _*)
    }
  }

  override def search(query: Query[D]): Stream[IO, SearchResult[D]] = {
    var q = lucene.query().offset(query.offset).limit(query.batchSize)
    q = query.filters.foldLeft(q)((qb, f) => q.filter(filter2Lucene(f)))
    q = q.sort(query.sort.map {
      case Sort.BestMatch => LuceneSort.Score
      case Sort.IndexOrder => LuceneSort.IndexOrder
      case Sort.ByField(field, reverse) => LuceneSort(this.field[Any](field.name).luceneField, reverse)
    }: _*)

    val pagedResults = q.search()
    val pagedResultsIterator = pagedResults.pagedResultsIterator
    Stream.fromBlockingIterator[IO](pagedResultsIterator, query.batchSize)
      .map(result => LuceneSearchResult(query, pagedResults.total, result))
  }

  override def truncate(): IO[Unit] = IO(lucene.delete(MatchAllSearchTerm))

  override def dispose(): IO[Unit] = IO(lucene.dispose())

  case class IndexedField[F](luceneField: LuceneField[F], field: Field[D, F]) {
    def fieldAndValue(value: D): FieldAndValue[F] = luceneField(field.getter(value))
  }

  case class LuceneSearchResult(query: Query[D],
                                total: Long,
                                result: L4SSearchResult) extends SearchResult[D] {
    override lazy val id: Id[D] = result(_id.luceneField)

    override def get(): IO[D] = collection(id)

    override def apply[F](field: Field[D, F]): F = {
      val indexedField = fields.find(_.field.name == field.name).getOrElse(throw new RuntimeException(s"Unable to find indexed field for: ${field.name}"))
      result(indexedField.luceneField).asInstanceOf[F]
    }
  }
}