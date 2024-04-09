package lightdb.index

import lightdb.query.Filter
import lightdb.{Collection, Document}
import org.apache.lucene.index.Term
import org.apache.lucene.search.{SortField, TermQuery}
import org.apache.lucene.{document => ld}

case class StringField[D <: Document[D]](fieldName: String,
                                         collection: Collection[D],
                                         get: D => String,
                                         store: Boolean) extends IndexedField[String, D] {
  def ===(value: String): Filter[D] = is(value)

  def is(value: String): Filter[D] = Filter(new TermQuery(new Term(fieldName, value)))

  override protected[lightdb] def createFields(doc: D): List[ld.Field] = List(
    new ld.StringField(fieldName, get(doc), if (store) ld.Field.Store.YES else ld.Field.Store.NO)
  )

  override protected[lightdb] def sortType: SortField.Type = SortField.Type.STRING
}
