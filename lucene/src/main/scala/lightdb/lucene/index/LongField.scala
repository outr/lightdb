package lightdb.lucene.index

import lightdb.index.IndexedField
import lightdb.lucene.LuceneIndexedField
import lightdb.{Collection, Document}
import org.apache.lucene.document.Field
import org.apache.lucene.search.SortField
import org.apache.lucene.{document => ld}

case class LongField[D <: Document[D]](fieldName: String,
                                       collection: Collection[D],
                                       get: D => Option[Long]) extends LuceneIndexedField[Long, D] {
  override protected[lightdb] def createFields(doc: D): List[Field] = get(doc).toList.map { value =>
    new ld.LongField(fieldName, value, Field.Store.NO)
  }

  override protected[lightdb] def sortType: SortField.Type = SortField.Type.LONG
}
