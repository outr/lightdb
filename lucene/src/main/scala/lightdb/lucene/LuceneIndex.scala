//package lightdb.lucene
//
//import fabric._
//import fabric.define.DefType
//import fabric.rw._
//import lightdb.aggregate.AggregateFilter
//import lightdb.document.Document
//import lightdb.filter.{Filter, FilterSupport}
//import lightdb.index.{Index, Indexer}
//import lightdb.spatial.GeoPoint
//import org.apache.lucene.document.{DoubleField, DoublePoint, Field, IntField, IntPoint, LatLonDocValuesField, LatLonPoint, LongField, LongPoint, StringField, TextField}
//import org.apache.lucene.index.Term
//import org.apache.lucene.search._
//import org.apache.lucene.queryparser.classic.QueryParser
//
//import scala.language.implicitConversions
//
//case class LuceneIndex[F, D <: Document[D]](name: String,
//                                            indexer: LuceneIndexer[D, _],
//                                            get: D => List[F],
//                                            store: Boolean,
//                                            sorted: Boolean,
//                                            tokenized: Boolean)
//                                           (implicit val rw: RW[F]) extends Index[F, D] {
//  private implicit def filter2Lucene(filter: Filter[D]): LuceneFilter[D] = filter.asInstanceOf[LuceneFilter[D]]
//
//  lazy val fieldSortName: String = {
//    val separate = rw.definition.className.collect {
//      case "lightdb.spatial.GeoPoint" => true
//    }.getOrElse(false)
//    if (separate) s"${name}Sort" else name
//  }
//
//  override def is(value: F): Filter[D] = LuceneFilter(() => value.json match {
//    case Str(s, _) if tokenized =>
//      val b = new BooleanQuery.Builder
//      s.split("\\s+").foreach(s => b.add(new TermQuery(new Term(name, s)), BooleanClause.Occur.MUST))
//      b.build()
//    case Str(s, _) => new TermQuery(new Term(name, s))
//    case Bool(b, _) => IntPoint.newExactQuery(name, if (b) 1 else 0)
//    case NumInt(l, _) => LongPoint.newExactQuery(name, l)
//    case NumDec(bd, _) => DoublePoint.newExactQuery(name, bd.toDouble)
//    case json => throw new RuntimeException(s"Unsupported equality check: $json (${rw.definition})")
//  })
//
//  override def IN(values: Seq[F]): LuceneFilter[D] = {
//    val b = new BooleanQuery.Builder
//    b.setMinimumNumberShouldMatch(1)
//    values.foreach { value =>
//      b.add(is(value).asQuery(), BooleanClause.Occur.SHOULD)
//    }
//    LuceneFilter(() => b.build())
//  }
//
//  override def rangeLong(from: Long, to: Long): Filter[D] = LuceneFilter(() => LongField.newRangeQuery(
//    name,
//    from,
//    to
//  ))
//
//  override def rangeDouble(from: Double, to: Double): Filter[D] = LuceneFilter(() => DoubleField.newRangeQuery(
//    name,
//    from,
//    to
//  ))
//
//  override def parsed(query: String, allowLeadingWildcard: Boolean = false): Filter[D] = {
//    LuceneFilter(() => {
//      val parser = new QueryParser(name, indexer.analyzer)
//      parser.setAllowLeadingWildcard(allowLeadingWildcard)
//      parser.setSplitOnWhitespace(true)
//      parser.parse(query)
//    })
//  }
//
//  override def words(s: String,
//            matchStartsWith: Boolean = true,
//            matchEndsWith: Boolean = false): Filter[D] = {
//    val words = s.split("\\s+").map { w =>
//      if (matchStartsWith && matchEndsWith) {
//        s"%$w%"
//      } else if (matchStartsWith) {
//        s"%$w"
//      } else if (matchEndsWith) {
//        s"$w%"
//      } else {
//        w
//      }
//    }.mkString(" ")
//    parsed(words, allowLeadingWildcard = matchEndsWith)
//  }
//}