package lightdb.lucene

import fabric.define.DefType
import fabric.io.JsonFormatter
import fabric.rw.Asable
import fabric.{Arr, Json, Null, NumDec, NumInt, Str}
import lightdb.aggregate.AggregateQuery
import lightdb.{Id, Query, SearchResults}
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.{FacetField, Tokenized}
import lightdb.field.{Field, IndexingState}
import lightdb.filter.Filter
import lightdb.materialized.MaterializedAggregate
import lightdb.spatial.Geo
import lightdb.store.Conversion
import lightdb.transaction.{CollectionTransaction, Transaction}
import lightdb.util.Aggregator
import org.apache.lucene.document.{DoubleDocValuesField, DoubleField, IntField, LatLonDocValuesField, LatLonPoint, LatLonShape, LongField, NumericDocValuesField, SortedDocValuesField, StoredField, StringField, TextField, Document => LuceneDocument, Field => LuceneField}
import org.apache.lucene.facet.{FacetField => LuceneFacetField}
import org.apache.lucene.geo.{Line, Polygon}
import org.apache.lucene.index.Term
import org.apache.lucene.search.MatchAllDocsQuery
import org.apache.lucene.util.BytesRef
import rapid.Task

case class LuceneTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: LuceneStore[Doc, Model],
                                                                                state: LuceneState[Doc],
                                                                                parent: Option[Transaction[Doc, Model]]) extends CollectionTransaction[Doc, Model] {
  private lazy val searchBuilder = new LuceneSearchBuilder[Doc, Model](store, store.model, this)

  override def jsonStream: rapid.Stream[Json] =
    rapid.Stream.force(doSearch[Json](Query[Doc, Model, Json](this, Conversion.Json(store.fields))).map(_.stream))

  override protected def _get[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = {
    val filter = Filter.Equals(index, value)
    val query = Query[Doc, Model, Doc](this, Conversion.Doc(), filter = Some(filter), limit = Some(1))
    doSearch[Doc](query).flatMap(_.list).map(_.headOption)
  }

  override protected def _insert(doc: Doc): Task[Doc] = addDoc(doc, upsert = false)

  override protected def _upsert(doc: Doc): Task[Doc] = addDoc(doc, upsert = true)

  override protected def _exists(id: Id[Doc]): Task[Boolean] = get(id).map(_.nonEmpty)

  override protected def _count: Task[Int] = Task(state.indexSearcher.count(new MatchAllDocsQuery))

  override protected def _delete[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Boolean] = Task {
    val query = searchBuilder.filter2Lucene(Some(index === value))
    store.index.indexWriter.deleteDocuments(query)
    true
  }

  override protected def _commit: Task[Unit] = state.commit

  override protected def _rollback: Task[Unit] = state.rollback

  override protected def _close: Task[Unit] = state.close

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] = searchBuilder(query)

  override def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    Aggregator(query, store.model)

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] = aggregate(query).count

  override def truncate: Task[Int] = for {
    count <- this.count
    _ <- Task(store.index.indexWriter.deleteAll())
  } yield count

  private def addDoc(doc: Doc, upsert: Boolean): Task[Doc] = Task {
    if (store.fields.tail.nonEmpty) {
      val id = doc._id
      val state = new IndexingState
      val luceneFields = store.fields.flatMap { field =>
        createLuceneFields(field, doc, state)
      }
      val document = new LuceneDocument
      luceneFields.foreach(document.add)

      if (upsert) {
        store.index.indexWriter.updateDocument(new Term("_id", id.value), facetsPrepareDoc(document))
      } else {
        store.index.indexWriter.addDocument(facetsPrepareDoc(document))
      }
    }
    doc
  }

  private def createLuceneFields(field: Field[Doc, _], doc: Doc, state: IndexingState): List[LuceneField] = {
    def fs: LuceneField.Store = if (store.storeMode.isAll || field.indexed) LuceneField.Store.YES else LuceneField.Store.NO
    val json = field.getJson(doc, state)
    var fields = List.empty[LuceneField]
    def add(field: LuceneField): Unit = fields = field :: fields
    field match {
      case ff: FacetField[Doc] => ff.get(doc, ff, state).flatMap { value =>
        if (value.path.nonEmpty || ff.hierarchical) {
          val path = if (ff.hierarchical) value.path ::: List("$ROOT$") else value.path
          Some(new LuceneFacetField(field.name, path: _*))
        } else {
          None
        }
      }
      case t: Tokenized[Doc] =>
        List(new LuceneField(field.name, t.get(doc, t, state), if (fs == LuceneField.Store.YES) TextField.TYPE_STORED else TextField.TYPE_NOT_STORED))
      case _ =>
        def addJson(json: Json, d: DefType): Unit = {
          if (field.isSpatial) {
            if (json != Null) try {
              createGeoFields(field, json, add)
            } catch {
              case t: Throwable => throw new RuntimeException(s"Failure to populate geo field '${store.name}.${field.name}' for $doc (json: $json, className: ${field.className})", t)
            }
          } else {
            d match {
              case DefType.Str => json match {
                case Null => add(new StringField(field.name, Field.NullString, fs))
                case _ => add(new StringField(field.name, json.asString, fs))
              }
              case DefType.Enum(_, _) => add(new StringField(field.name, json.asString, fs))
              case DefType.Opt(d) => addJson(json, d)
              case DefType.Json | DefType.Obj(_, _) => add(new StringField(field.name, JsonFormatter.Compact(json), fs))
              case _ if json == Null => // Ignore null values
              case DefType.Arr(d) =>
                val v = json.asVector
                if (v.isEmpty) {
                  add(new StringField(field.name, "[]", fs))
                } else {
                  v.foreach(json => addJson(json, d))
                }
              case DefType.Bool => add(new IntField(field.name, if (json.asBoolean) 1 else 0, fs))
              case DefType.Int => add(new LongField(field.name, json.asLong, fs))
              case DefType.Dec => add(new DoubleField(field.name, json.asDouble, fs))
              case _ => throw new UnsupportedOperationException(s"Unsupported definition (field: ${field.name}, className: ${field.className}): $d for $json")
            }
          }
        }
        addJson(json, field.rw.definition)

        val fieldSortName = s"${field.name}Sort"
        field.getJson(doc, state) match {
          case Str(s, _) =>
            val bytes = new BytesRef(s)
            val sorted = new SortedDocValuesField(fieldSortName, bytes)
            add(sorted)
          case NumInt(l, _) => add(new NumericDocValuesField(fieldSortName, l))
          case NumDec(d, _) => add(new DoubleDocValuesField(fieldSortName, d.toDouble))
          case j if field.isSpatial && j != Null =>
            val list = j match {
              case Arr(values, _) => values.toList.map(_.as[Geo])
              case _ => List(j.as[Geo])
            }
            list.foreach { g =>
              add(new LatLonDocValuesField(fieldSortName, g.center.latitude, g.center.longitude))
            }
          case _ => // Ignore
        }
        fields
    }
  }

  private def createGeoFields(field: Field[Doc, _],
                              json: Json,
                              add: LuceneField => Unit): Unit = {
    field.className match {
      case _ =>
        def indexPoint(p: Geo.Point): Unit = try {
          LatLonShape.createIndexableFields(field.name, p.latitude, p.longitude)
        } catch {
          case t: Throwable => throw new RuntimeException(s"Failed to add LatLonPoint.createIndexableFields(${field.name}, ${p.latitude}, ${p.longitude}): ${JsonFormatter.Default(json)}", t)
        }
        def indexLine(l: Geo.Line): Unit = {
          val line = new Line(l.points.map(_.latitude).toArray, l.points.map(_.longitude).toArray)
          LatLonShape.createIndexableFields(field.name, line)
        }
        def indexPolygon(p: Geo.Polygon): Unit = {
          def convert(p: Geo.Polygon): Polygon =
            new Polygon(p.points.map(_.latitude).toArray, p.points.map(_.longitude).toArray)
          val polygon = convert(p)
          LatLonShape.createIndexableFields(field.name, polygon)
        }
        def indexGeo(geo: Geo): Unit = geo match {
          case p: Geo.Point => indexPoint(p)
          case Geo.MultiPoint(points) => points.foreach(indexPoint)
          case l: Geo.Line => indexLine(l)
          case Geo.MultiLine(lines) => lines.foreach(indexLine)
          case p: Geo.Polygon => indexPolygon(p)
          case Geo.MultiPolygon(polygons) => polygons.foreach(indexPolygon)
          case Geo.GeometryCollection(geometries) => geometries.foreach(indexGeo)
        }
        val list = json match {
          case Arr(value, _) => value.toList.map(_.as[Geo])
          case _ => List(json.as[Geo])
        }
        list.foreach { geo =>
          indexGeo(geo)
          add(new LatLonPoint(field.name, geo.center.latitude, geo.center.longitude))
        }
        if (list.isEmpty) {
          add(new LatLonPoint(field.name, 0.0, 0.0))
        }
    }
    add(new StoredField(field.name, JsonFormatter.Compact(json)))
  }

  private def facetsPrepareDoc(doc: LuceneDocument): LuceneDocument = if (store.hasFacets) {
    store.facetsConfig.build(store.index.taxonomyWriter, doc)
  } else {
    doc
  }
}
