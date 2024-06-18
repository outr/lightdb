package lightdb.lucene

import fabric._
import fabric.define.DefType
import fabric.rw._
import lightdb.aggregate.AggregateFilter
import lightdb.document.Document
import lightdb.filter.{Filter, FilterSupport}
import lightdb.index.{Index, Indexer}
import lightdb.spatial.GeoPoint
import org.apache.lucene.document.{DoubleField, DoublePoint, Field, IntField, IntPoint, LatLonDocValuesField, LatLonPoint, LongField, LongPoint, StringField, TextField}
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.apache.lucene.queryparser.classic.QueryParser

import scala.language.implicitConversions

case class LuceneIndex[F, D <: Document[D]](name: String,
                                            indexer: LuceneIndexer[D],
                                            get: D => List[F],
                                            store: Boolean,
                                            sorted: Boolean,
                                            tokenized: Boolean)
                                           (implicit val rw: RW[F]) extends Index[F, D] {
  private implicit def filter2Lucene(filter: Filter[D]): LuceneFilter[D] = filter.asInstanceOf[LuceneFilter[D]]

  lazy val fieldSortName: String = {
    val separate = rw.definition.className.collect {
      case "lightdb.spatial.GeoPoint" => true
    }.getOrElse(false)
    if (separate) s"${name}Sort" else name
  }

  override def is(value: F): Filter[D] = LuceneFilter(() => value.json match {
    case Str(s, _) if tokenized =>
      val b = new BooleanQuery.Builder
      s.split("\\s+").foreach(s => b.add(new TermQuery(new Term(name, s)), BooleanClause.Occur.MUST))
      b.build()
    case Str(s, _) => new TermQuery(new Term(name, s))
    case Bool(b, _) => IntPoint.newExactQuery(name, if (b) 1 else 0)
    case NumInt(l, _) => LongPoint.newExactQuery(name, l)
    case NumDec(bd, _) => DoublePoint.newExactQuery(name, bd.toDouble)
    case json => throw new RuntimeException(s"Unsupported equality check: $json (${rw.definition})")
  })

  override def IN(values: Seq[F]): LuceneFilter[D] = {
    val b = new BooleanQuery.Builder
    b.setMinimumNumberShouldMatch(1)
    values.foreach { value =>
      b.add(is(value).asQuery(), BooleanClause.Occur.SHOULD)
    }
    LuceneFilter(() => b.build())
  }

  override def rangeLong(from: Long, to: Long): Filter[D] = LuceneFilter(() => LongField.newRangeQuery(
    name,
    from,
    to
  ))

  override def rangeDouble(from: Double, to: Double): Filter[D] = LuceneFilter(() => DoubleField.newRangeQuery(
    name,
    from,
    to
  ))

  override def parsed(query: String, allowLeadingWildcard: Boolean = false): Filter[D] = {
    LuceneFilter(() => {
      val parser = new QueryParser(name, indexer.analyzer)
      parser.setAllowLeadingWildcard(allowLeadingWildcard)
      parser.setSplitOnWhitespace(true)
      parser.parse(query)
    })
  }

  override def words(s: String,
            matchStartsWith: Boolean = true,
            matchEndsWith: Boolean = false): Filter[D] = {
    val words = s.split("\\s+").map { w =>
      if (matchStartsWith && matchEndsWith) {
        s"%$w%"
      } else if (matchStartsWith) {
        s"%$w"
      } else if (matchEndsWith) {
        s"$w%"
      } else {
        w
      }
    }.mkString(" ")
    parsed(words, allowLeadingWildcard = matchEndsWith)
  }

  protected[lightdb] def createFields(doc: D): List[Field] = if (tokenized) {
    getJson(doc).flatMap {
      case Null => Nil
      case Str(s, _) => List(s)
      case f => throw new RuntimeException(s"Unsupported tokenized value: $f (${rw.definition})")
    }.map { value =>
      new Field(name, value, if (store) TextField.TYPE_STORED else TextField.TYPE_NOT_STORED)
    }
  } else {
    def fs: Field.Store = if (store) Field.Store.YES else Field.Store.NO

    val filterField = getJson(doc).flatMap {
      case Null => None
      case Str(s, _) => Some(new StringField(name, s, fs))
      case Bool(b, _) => Some(new IntField(name, if (b) 1 else 0, fs))
      case NumInt(l, _) => Some(new LongField(name, l, fs))
      case NumDec(bd, _) => Some(new DoubleField(name, bd.toDouble, fs))
      case obj: Obj if obj.reference.nonEmpty => obj.reference.get match {
        case GeoPoint(latitude, longitude) => Some(new LatLonPoint(name, latitude, longitude))
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

  override def aggregate(name: String): FilterSupport[F, D, AggregateFilter[D]] =
    throw new UnsupportedOperationException("Aggregation is not currently supported for Lucene")
}