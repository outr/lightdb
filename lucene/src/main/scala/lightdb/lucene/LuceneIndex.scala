package lightdb.lucene

import fabric._
import fabric.define.DefType
import fabric.rw._
import lightdb.Document
import lightdb.index.{IndexSupport, IndexedField}
import org.apache.lucene.document.Field
import org.apache.lucene.index.Term
import org.apache.lucene.search._
import org.apache.lucene.{document => ld}

case class LuceneIndex[F, D <: Document[D]](fieldName: String,
                                            indexSupport: IndexSupport[D],
                                            get: D => List[F],
                                            store: Boolean,
                                            tokenized: Boolean)
                                           (implicit val rw: RW[F]) extends IndexedField[F, D] {
  def ===(value: F): LuceneFilter[D] = is(value)
  def is(value: F): LuceneFilter[D] = LuceneFilter(() => value.json match {
    case Str(s, _) => new TermQuery(new Term(fieldName, s))
    case json => throw new RuntimeException(s"Unsupported equality check: $json (${rw.definition})")
  })

  def IN(values: Seq[F]): LuceneFilter[D] = {
    val b = new BooleanQuery.Builder
    b.setMinimumNumberShouldMatch(1)
    values.foreach { value =>
      b.add(is(value).asQuery(), BooleanClause.Occur.SHOULD)
    }
    LuceneFilter(() => b.build())
  }

  def between(lower: F, upper: F): LuceneFilter[D] = LuceneFilter(() => (lower.json, upper.json) match {
    case (NumInt(l, _), NumInt(u, _)) => ld.LongField.newRangeQuery(fieldName, l, u)
    case _ => throw new RuntimeException(s"Unsupported between for $lower - $upper (${rw.definition})")
  })

  protected[lightdb] def createFields(doc: D): List[ld.Field] = if (tokenized) {
    getJson(doc).flatMap {
      case Null => Nil
      case Str(s, _) => List(s)
      case f => throw new RuntimeException(s"Unsupported tokenized value: $f (${rw.definition})")
    }.map { value =>
      new ld.Field(fieldName, value, if (store) ld.TextField.TYPE_STORED else ld.TextField.TYPE_NOT_STORED)
    }
  } else {
    def fs: Field.Store = if (store) ld.Field.Store.YES else ld.Field.Store.NO

    getJson(doc).flatMap {
      case Null => None
      case Str(s, _) => Some(new ld.StringField(fieldName, s, fs))
      case Bool(b, _) => Some(new ld.StringField(fieldName, b.toString, fs))
      case NumInt(l, _) => Some(new ld.LongField(fieldName, l, fs))
      case NumDec(bd, _) => Some(new ld.StringField(fieldName, bd.toString(), fs))
      case json => throw new RuntimeException(s"Unsupported JSON: $json (${rw.definition})")
    }
  }

  protected[lightdb] def sortType: SortField.Type = rw.definition match {
    case DefType.Str => SortField.Type.STRING
    case DefType.Dec => SortField.Type.DOUBLE
    case DefType.Int => SortField.Type.LONG
    case _ => throw new RuntimeException(s"Unsupported sort type for ${rw.definition}")
  }
}