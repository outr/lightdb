package lightdb.lucene.blockjoin

import fabric.Json
import fabric.define.DefType
import fabric.io.JsonFormatter
import fabric.rw.Asable
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.field.Field.Tokenized
import lightdb.field.IndexingState
import lightdb.spatial.{Geo, GeometryCollection, Line, MultiLine, MultiPoint, MultiPolygon, Point, Polygon}
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.document.{DoubleDocValuesField, DoubleField, IntField, LatLonDocValuesField, LatLonPoint, LatLonShape, LongField, NumericDocValuesField, SortedDocValuesField, StringField, TextField, Document => LuceneDocument, Field => LuceneField}
import org.apache.lucene.util.BytesRef
import rapid.Task

import java.util

/**
 * Helper for writing Lucene block-join documents (children first, parent last).
 *
 * This is intentionally separate from the regular LuceneTransaction insert/upsert path.
 */
object LuceneBlockJoinIndexer {
  def indexBlock[
    Parent <: Document[Parent],
    ParentModel <: DocumentModel[Parent],
    Child <: Document[Child],
    ChildModel <: DocumentModel[Child]
  ](indexWriter: IndexWriter,
    parentModel: ParentModel,
    parentFields: List[Field[Parent, _]],
    parentStoreAll: Boolean,
    parent: Parent,
    childModel: ChildModel,
    childFields: List[Field[Child, _]],
    childStoreAll: Boolean,
    children: List[Child]): Task[Unit] = Task {
    val state = new IndexingState

    def mkParentDoc(): LuceneDocument = {
      val d = new LuceneDocument
      d.add(new StringField(LuceneBlockJoinFields.TypeField, LuceneBlockJoinFields.ParentTypeValue, LuceneField.Store.NO))
      writeFields(parentModel, parentFields, parent, state, LuceneBlockJoinFields.mapParentFieldName, storeAll = parentStoreAll, add = d.add)
      d
    }

    def mkChildDoc(c: Child): LuceneDocument = {
      val d = new LuceneDocument
      d.add(new StringField(LuceneBlockJoinFields.TypeField, LuceneBlockJoinFields.ChildTypeValue, LuceneField.Store.NO))
      d.add(new StringField(LuceneBlockJoinFields.ParentIdField, parent._id.value, LuceneField.Store.NO))
      writeFields(childModel, childFields, c, state, LuceneBlockJoinFields.mapChildFieldName, storeAll = childStoreAll, add = d.add)
      d
    }

    val docs: util.List[LuceneDocument] = new util.ArrayList[LuceneDocument](children.size + 1)
    children.foreach(c => docs.add(mkChildDoc(c)))
    docs.add(mkParentDoc())

    indexWriter.addDocuments(docs)
  }

  private def writeFields[D <: Document[D], M <: DocumentModel[D]](model: M,
                                                                   fields: List[Field[D, _]],
                                                                   doc: D,
                                                                   state: IndexingState,
                                                                   mapName: String => String,
                                                                   storeAll: Boolean,
                                                                   add: LuceneField => Unit): Unit = {
    fields.foreach { field =>
      val json = field.getJson(doc, state)
      val fieldName = mapName(field.name)
      val sortFieldName = s"${fieldName}Sort"
      // Match LuceneTransaction storage rules: store for StoreMode.All, or when explicitly stored+indexed.
      // Always store _id so materialization can reconstruct ids.
      val storeField: LuceneField.Store = {
        val shouldStore =
          storeAll || (field.stored && field.indexed) || field.name == "_id"
        if shouldStore then LuceneField.Store.YES else LuceneField.Store.NO
      }

      field match {
        case t: Tokenized[D @unchecked] =>
          // Tokenized fields follow storeField when StoreMode.All or explicitly stored+indexed.
          add(new LuceneField(fieldName, t.get(doc, t, state), if storeField == LuceneField.Store.YES then TextField.TYPE_STORED else TextField.TYPE_NOT_STORED))
        case _ =>
          def addJson(j: Json, d: DefType): Unit = {
            if field.isSpatial then {
              if j != fabric.Null then createGeoFields(fieldName, j, add)
            } else {
              d match {
                case DefType.Str =>
                  j match {
                    case fabric.Null => add(new StringField(fieldName, Field.NullString, storeField))
                    case _ => add(new StringField(fieldName, j.asString, storeField))
                  }
                case DefType.Enum(_, _) =>
                  add(new StringField(fieldName, j.asString, storeField))
                case DefType.Opt(d2) =>
                  addJson(j, d2)
                case DefType.Json | DefType.Obj(_, _) | DefType.Poly(_, _) =>
                  add(new StringField(fieldName, JsonFormatter.Compact(j), storeField))
                case _ if j == fabric.Null =>
                case DefType.Arr(d2) =>
                  val v = j.asVector
                  if v.isEmpty then add(new StringField(fieldName, "[]", storeField))
                  else v.foreach(x => addJson(x, d2))
                case DefType.Bool =>
                  add(new IntField(fieldName, if j.asBoolean then 1 else 0, storeField))
                case DefType.Int =>
                  add(new LongField(fieldName, j.asLong, storeField))
                case DefType.Dec =>
                  add(new DoubleField(fieldName, j.asDouble, storeField))
                case _ =>
                  throw new UnsupportedOperationException(s"Unsupported definition (field: ${field.name}, className: ${field.className}): $d for $j")
              }
            }
          }

          addJson(json, field.rw.definition)

          json match {
            case fabric.Str(s, _) =>
              add(new SortedDocValuesField(sortFieldName, new BytesRef(s)))
            case fabric.NumInt(l, _) =>
              add(new NumericDocValuesField(sortFieldName, l))
            case fabric.NumDec(d, _) =>
              add(new DoubleDocValuesField(sortFieldName, d.toDouble))
            case j if field.isSpatial && j != fabric.Null =>
              val list = j match {
                case fabric.Arr(values, _) => values.toList.map(_.as[Geo])
                case _ => List(j.as[Geo])
              }
              list.foreach { g =>
                add(new LatLonDocValuesField(sortFieldName, g.center.latitude, g.center.longitude))
              }
            case _ =>
          }
      }
    }
  }

  private def createGeoFields(fieldName: String, json: Json, add: LuceneField => Unit): Unit = {
    def indexPoint(p: Point): Unit =
      LatLonShape.createIndexableFields(fieldName, p.latitude, p.longitude).foreach(add)
    def indexLine(l: Line): Unit = {
      val line = new org.apache.lucene.geo.Line(l.points.map(_.latitude).toArray, l.points.map(_.longitude).toArray)
      LatLonShape.createIndexableFields(fieldName, line).foreach(add)
    }
    def indexPolygon(p: Polygon): Unit = {
      def convert(p: Polygon): org.apache.lucene.geo.Polygon =
        new org.apache.lucene.geo.Polygon(p.points.map(_.latitude).toArray, p.points.map(_.longitude).toArray)
      LatLonShape.createIndexableFields(fieldName, convert(p)).foreach(add)
    }
    def indexGeo(geo: Geo): Unit = geo match {
      case p: Point => indexPoint(p)
      case MultiPoint(points) => points.foreach(indexPoint)
      case l: Line => indexLine(l)
      case MultiLine(lines) => lines.foreach(indexLine)
      case p: Polygon => indexPolygon(p)
      case MultiPolygon(polygons) => polygons.foreach(indexPolygon)
      case GeometryCollection(geometries) => geometries.foreach(indexGeo)
    }

    val geo = json.as[Geo]
    indexGeo(geo)
    val center = geo.center
    add(new LatLonPoint(fieldName, center.latitude, center.longitude))
  }
}


