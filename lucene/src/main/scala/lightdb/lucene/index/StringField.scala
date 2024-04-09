package lightdb.lucene.index

import lightdb.lucene.{LuceneFilter, LuceneIndexedField}
import lightdb.query.Filter
import lightdb.{Collection, Document}
import org.apache.lucene.index.Term
import org.apache.lucene.search.{BooleanClause, BooleanQuery, SortField, TermQuery}
import org.apache.lucene.{document => ld}

case class StringField[D <: Document[D]](fieldName: String,
                                         collection: Collection[D],
                                         get: D => String,
                                         store: Boolean) extends LuceneIndexedField[String, D] {
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

  override protected[lightdb] def createFields(doc: D): List[ld.Field] = List(
    new ld.StringField(fieldName, get(doc), if (store) ld.Field.Store.YES else ld.Field.Store.NO)
  )

  override protected[lightdb] def sortType: SortField.Type = SortField.Type.STRING
}
