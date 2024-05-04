package lightdb.lucene.index

import fabric.rw.RW
import lightdb.lucene.{LuceneFilter, LuceneIndexedField}
import lightdb.query.Filter
import lightdb.Document
import lightdb.model.Collection
import org.apache.lucene.index.Term
import org.apache.lucene.search.{BooleanClause, BooleanQuery, SortField, TermQuery}
import org.apache.lucene.{document => ld}

case class StringField[D <: Document[D]](fieldName: String,
                                         collection: Collection[D],
                                         get: D => Option[String],
                                         store: Boolean)
                                        (implicit val rw: RW[String]) extends LuceneIndexedField[String, D] {
  def ===(value: String): Filter[D] = is(value)

  def is(value: String): Filter[D] = LuceneFilter(() => new TermQuery(new Term(fieldName, value)))

  def includedIn(values: Seq[String]): Filter[D] = {
    val b = new BooleanQuery.Builder
    b.setMinimumNumberShouldMatch(1)
    values.foreach { value =>
      b.add(is(value).asInstanceOf[LuceneFilter[D]].asQuery(), BooleanClause.Occur.SHOULD)
    }
    LuceneFilter(() => b.build())
  }

  override protected[lightdb] def createFields(doc: D): List[ld.Field] = get(doc).toList.map { value =>
    new ld.StringField(fieldName, value, if (store) ld.Field.Store.YES else ld.Field.Store.NO)
  }

  override protected[lightdb] def sortType: SortField.Type = SortField.Type.STRING
}
