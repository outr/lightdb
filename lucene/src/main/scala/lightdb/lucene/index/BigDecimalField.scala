package lightdb.lucene.index

import fabric.rw.RW
import lightdb.lucene.LuceneIndexedField
import lightdb.Document
import lightdb.index.IndexSupport
import lightdb.model.{AbstractCollection, Collection}
import org.apache.lucene.document.Field
import org.apache.lucene.search.SortField
import org.apache.lucene.{document => ld}

case class BigDecimalField[D <: Document[D]](fieldName: String,
                                             indexSupport: IndexSupport[D],
                                             get: D => Option[BigDecimal])
                                            (implicit val rw: RW[BigDecimal]) extends LuceneIndexedField[BigDecimal, D] {
  override protected[lightdb] def createFields(doc: D): List[Field] = get(doc).toList.map { value =>
    new ld.StringField(fieldName, value.toString(), Field.Store.NO)
  }

  override protected[lightdb] def sortType: SortField.Type = SortField.Type.STRING
}
