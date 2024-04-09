package lightdb.lucene.index

import lightdb.index.IndexedField
import lightdb.lucene.LuceneIndexedField
import lightdb.query.Filter
import lightdb.{Collection, Document}
import org.apache.lucene.document.Field
import org.apache.lucene.search.SortField
import org.apache.lucene.{document => ld}

case class IntField[D <: Document[D]](fieldName: String,
                                      collection: Collection[D],
                                      get: D => Int) extends LuceneIndexedField[Int, D] {
  def ===(value: Int): Filter[D] = is(value)

  def is(value: Int): Filter[D] = Filter(ld.IntField.newExactQuery(fieldName, value))

  def between(lower: Int, upper: Int): Filter[D] = Filter(ld.IntField.newRangeQuery(fieldName, lower, upper))

  override protected[lightdb] def createFields(doc: D): List[Field] = List(
    new ld.IntField(fieldName, get(doc), Field.Store.NO)
  )

  override protected[lightdb] def sortType: SortField.Type = SortField.Type.INT
}
