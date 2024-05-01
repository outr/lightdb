package lightdb.lucene.index

import lightdb.lucene.{LuceneFilter, LuceneIndexedField}
import lightdb.query.Filter
import lightdb.Document
import lightdb.model.Collection
import org.apache.lucene.document.Field
import org.apache.lucene.search.SortField
import org.apache.lucene.{document => ld}

case class IntField[D <: Document[D]](fieldName: String,
                                      collection: Collection[D],
                                      get: D => Option[Int]) extends LuceneIndexedField[Int, D] {
  def ===(value: Int): Filter[D] = is(value)

  def is(value: Int): Filter[D] = LuceneFilter(() => ld.IntField.newExactQuery(fieldName, value))

  def between(lower: Int, upper: Int): Filter[D] = LuceneFilter(() => ld.IntField.newRangeQuery(fieldName, lower, upper))

  override protected[lightdb] def createFields(doc: D): List[Field] = get(doc).toList.map { value =>
    new ld.IntField(fieldName, value, Field.Store.NO)
  }

  override protected[lightdb] def sortType: SortField.Type = SortField.Type.INT
}
