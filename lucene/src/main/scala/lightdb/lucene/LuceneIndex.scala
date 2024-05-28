package lightdb.lucene

import fabric._
import fabric.define.DefType
import fabric.rw._
import lightdb.Document
import lightdb.index.{IndexSupport, IndexedField}
import lightdb.spatial.GeoPoint
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.apache.lucene.document._
import org.apache.lucene.queryparser.classic.QueryParser

case class LuceneIndex[F, D <: Document[D]](fieldName: String,
                                            indexSupport: IndexSupport[D],
                                            get: D => List[F],
                                            store: Boolean,
                                            sorted: Boolean,
                                            tokenized: Boolean)
                                           (implicit val rw: RW[F]) extends IndexedField[F, D] {
  private def lucene: LuceneSupport[D] = indexSupport.asInstanceOf[LuceneSupport[D]]

  lazy val fieldSortName: String = {
    val separate = rw.definition.className.collect {
      case "lightdb.spatial.GeoPoint" => true
    }.getOrElse(false)
    if (separate) s"${fieldName}Sort" else fieldName
  }

  def ===(value: F): LuceneFilter[D] = is(value)
  def is(value: F): LuceneFilter[D] = LuceneFilter(() => value.json match {
    case Str(s, _) if tokenized =>
      val b = new BooleanQuery.Builder
      s.split("\\s+").foreach(s => b.add(new TermQuery(new Term(fieldName, s)), BooleanClause.Occur.MUST))
      b.build()
    case Str(s, _) => new TermQuery(new Term(fieldName, s))
    case json => throw new RuntimeException(s"Unsupported equality check: $json (${rw.definition})")
  })

  def parsed(query: String, allowLeadingWildcard: Boolean = false): LuceneFilter[D] = {
    LuceneFilter(() => {
      val parser = new QueryParser(fieldName, lucene.index.analyzer)
      parser.setAllowLeadingWildcard(allowLeadingWildcard)
      parser.parse(query)
    })
  }

  def IN(values: Seq[F]): LuceneFilter[D] = {
    val b = new BooleanQuery.Builder
    b.setMinimumNumberShouldMatch(1)
    values.foreach { value =>
      b.add(is(value).asQuery(), BooleanClause.Occur.SHOULD)
    }
    LuceneFilter(() => b.build())
  }

  def between(lower: F, upper: F): LuceneFilter[D] = LuceneFilter(() => (lower.json, upper.json) match {
    case (NumInt(l, _), NumInt(u, _)) => LongField.newRangeQuery(fieldName, l, u)
    case _ => throw new RuntimeException(s"Unsupported between for $lower - $upper (${rw.definition})")
  })

  protected[lightdb] def createFields(doc: D): List[Field] = if (tokenized) {
    getJson(doc).flatMap {
      case Null => Nil
      case Str(s, _) => List(s)
      case f => throw new RuntimeException(s"Unsupported tokenized value: $f (${rw.definition})")
    }.map { value =>
      new Field(fieldName, value, if (store) TextField.TYPE_STORED else TextField.TYPE_NOT_STORED)
    }
  } else {
    def fs: Field.Store = if (store) Field.Store.YES else Field.Store.NO

    val filterField = getJson(doc).flatMap {
      case Null => None
      case Str(s, _) => Some(new StringField(fieldName, s, fs))
      case Bool(b, _) => Some(new StringField(fieldName, b.toString, fs))
      case NumInt(l, _) => Some(new LongField(fieldName, l, fs))
      case NumDec(bd, _) => Some(new DoubleField(fieldName, bd.toDouble, fs))
      case obj: Obj if obj.reference.nonEmpty => obj.reference.get match {
        case GeoPoint(latitude, longitude) => Some(new LatLonPoint(fieldName, latitude, longitude))
        case ref => throw new RuntimeException(s"Unsupported object reference: $ref for JSON: $obj")
      }
      case json => throw new RuntimeException(s"Unsupported JSON: $json (${rw.definition})")
    }
    val sortField = if (sorted) {
      getJson(doc).flatMap {
        case obj: Obj if obj.reference.nonEmpty => obj.reference.get match {
          case GeoPoint(latitude, longitude) => Some(new LatLonDocValuesField(fieldSortName, latitude, longitude))
          case _ => None
        }
        case _ => None
      }
    } else {
      Nil
    }
    filterField ::: sortField
  }

  protected[lightdb] def sortType: SortField.Type = rw.definition match {
    case DefType.Str => SortField.Type.STRING
    case DefType.Dec => SortField.Type.DOUBLE
    case DefType.Int => SortField.Type.LONG
    case _ => throw new RuntimeException(s"Unsupported sort type for ${rw.definition}")
  }
}