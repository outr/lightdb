package lightdb.lucene.index

import lightdb.lucene.LuceneIndexedField
import lightdb.{Collection, Document}
import org.apache.lucene.document.Field
import org.apache.lucene.search.SortField
import org.apache.lucene.{document => ld}

case class BigDecimalField[D <: Document[D]](fieldName: String,
                                             collection: Collection[D],
                                             get: D => BigDecimal) extends LuceneIndexedField[BigDecimal, D] {
  override protected[lightdb] def createFields(doc: D): List[Field] = List(
    new ld.StringField(fieldName, get(doc).toString(), Field.Store.NO)
  )

  override protected[lightdb] def sortType: SortField.Type = SortField.Type.STRING
}
