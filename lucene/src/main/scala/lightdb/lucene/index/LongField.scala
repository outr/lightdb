package lightdb.lucene.index

import fabric.rw.RW
import lightdb.index.{IndexSupport, IndexedField}
import lightdb.lucene.LuceneIndexedField
import lightdb.Document
import lightdb.model.{AbstractCollection, Collection}
import org.apache.lucene.document.Field
import org.apache.lucene.search.SortField
import org.apache.lucene.{document => ld}

case class LongField[D <: Document[D]](fieldName: String,
                                       indexSupport: IndexSupport[D],
                                       get: D => Option[Long])
                                      (implicit val rw: RW[Long]) extends LuceneIndexedField[Long, D] {
  override protected[lightdb] def createFields(doc: D): List[Field] = get(doc).toList.map { value =>
    new ld.LongField(fieldName, value, Field.Store.NO)
  }

  override protected[lightdb] def sortType: SortField.Type = SortField.Type.LONG
}
