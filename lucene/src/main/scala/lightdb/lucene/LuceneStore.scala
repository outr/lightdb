package lightdb.lucene

import fabric._
import fabric.define.DefType
import fabric.io.JsonFormatter
import fabric.rw.Asable
import lightdb._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field._
import lightdb.field.{Field, IndexingState}
import lightdb.filter.Filter
import lightdb.lucene.index.Index
import lightdb.materialized.MaterializedAggregate
import lightdb.spatial.Geo
import lightdb.store.{Conversion, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.util.Aggregator
import org.apache.lucene.document.{DoubleDocValuesField, DoubleField, IntField, LatLonDocValuesField, LatLonPoint, LatLonShape, LongField, NumericDocValuesField, SortedDocValuesField, StoredField, StringField, TextField, Document => LuceneDocument, Field => LuceneField}
import org.apache.lucene.facet.{FacetsConfig, FacetField => LuceneFacetField}
import org.apache.lucene.geo.{Line, Polygon}
import org.apache.lucene.index.{DirectoryReader, SegmentReader, Term}
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery}
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.{BytesRef, Version}
import rapid._

import java.nio.file.{Files, Path}
import scala.language.implicitConversions

class LuceneStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                     model: Model,
                                                                     directory: Option[Path],
                                                                     val storeMode: StoreMode[Doc, Model]) extends Store[Doc, Model](name, model) {
  IndexSearcher.setMaxClauseCount(10_000_000)

  lazy val index = Index(directory)
  lazy val facetsConfig: FacetsConfig = {
    val c = new FacetsConfig
    fields.foreach {
      case ff: FacetField[_] =>
        if (ff.hierarchical) c.setHierarchical(ff.name, ff.hierarchical)
        if (ff.multiValued) c.setMultiValued(ff.name, ff.multiValued)
        if (ff.requireDimCount) c.setRequireDimCount(ff.name, ff.requireDimCount)
      case _ => // Ignore
    }
    c
  }
  private lazy val hasFacets: Boolean = fields.exists(_.isInstanceOf[FacetField[_]])
  private def facetsPrepareDoc(doc: LuceneDocument): LuceneDocument = if (hasFacets) {
    facetsConfig.build(index.taxonomyWriter, doc)
  } else {
    doc
  }

  override protected def initialize(): Task[Unit] = Task {
    directory.foreach { path =>
      if (Files.exists(path)) {
        val directory = FSDirectory.open(path)
        val reader = DirectoryReader.open(directory)
        reader.leaves().forEach { leaf =>
          val dataVersion = leaf.reader().asInstanceOf[SegmentReader].getSegmentInfo.info.getVersion
          val latest = Version.LATEST
          if (latest != dataVersion) {
            // TODO: Support re-indexing
            scribe.warn(s"Data Version: $dataVersion, Latest Version: $latest")
          }
        }
      }
    }
  }

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task {
    transaction.put(
      key = StateKey[Doc],
      value = LuceneState[Doc](index, hasFacets)
    )
  }

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] =
    addDoc(doc, upsert = false)

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] =
    addDoc(doc, upsert = true)

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

  private def createLuceneFields(field: Field[Doc, _], doc: Doc, state: IndexingState): List[LuceneField] = {
    def fs: LuceneField.Store = if (storeMode.isAll || field.indexed) LuceneField.Store.YES else LuceneField.Store.NO
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
              case t: Throwable => throw new RuntimeException(s"Failure to populate geo field '$name.${field.name}' for $doc (json: $json, className: ${field.className})", t)
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

  private def addDoc(doc: Doc, upsert: Boolean): Task[Doc] = Task {
    if (fields.tail.nonEmpty) {
      val id = this.id(doc)
      val state = new IndexingState
      val luceneFields = fields.flatMap { field =>
        createLuceneFields(field, doc, state)
      }
      val document = new LuceneDocument
      luceneFields.foreach(document.add)

      if (upsert) {
        index.indexWriter.updateDocument(new Term("_id", id.value), facetsPrepareDoc(document))
      } else {
        index.indexWriter.addDocument(facetsPrepareDoc(document))
      }
    }
    doc
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = get(idField, id).map(_.nonEmpty)

  override def get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = {
    val filter = Filter.Equals(field, value)
    val query = Query[Doc, Model, Doc](model, this, Conversion.Doc(), filter = Some(filter), limit = Some(1))
    doSearch[Doc](query).flatMap(_.list).map(_.headOption)
  }

  override def delete[V](field: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    val query = searchBuilder.filter2Lucene(Some(field === value))
    index.indexWriter.deleteDocuments(query)
    true
  }

  override def count(implicit transaction: Transaction[Doc]): Task[Int] =
    Task(state.indexSearcher.count(new MatchAllDocsQuery))

  override def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] =
    rapid.Stream.force(doSearch[Doc](Query[Doc, Model, Doc](model, this, Conversion.Doc())).map(_.stream))

  private lazy val searchBuilder = new LuceneSearchBuilder[Doc, Model](this, model)

  override def doSearch[V](query: Query[Doc, Model, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] = searchBuilder(query)

  override def optimize(): Task[Unit] = Task {
    val s = index.createIndexSearcher()
    val currentSegments = try {
      s.getIndexReader.leaves().size()
    } finally {
      index.releaseIndexSearch(s)
    }
    scribe.info(s"Optimizing Lucene Index for $name. Current segment count: $currentSegments")
    index.indexWriter.forceMerge(1)
  }

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    Aggregator(query, model)

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int] =
    aggregate(query).count

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = for {
    count <- this.count
    _ <- Task(index.indexWriter.deleteAll())
  } yield count

  override protected def doDispose(): Task[Unit] = Task {
    index.dispose()
  }
}

object LuceneStore extends StoreManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] =
    new LuceneStore[Doc, Model](name, model, db.directory.map(_.resolve(s"$name.lucene")), storeMode)
}