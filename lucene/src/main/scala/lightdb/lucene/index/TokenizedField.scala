package lightdb.lucene.index

import fabric.rw.RW
import lightdb.index.{IndexSupport, IndexedField}
import lightdb.lucene.LuceneIndexedField
import lightdb.Document
import lightdb.model.{AbstractCollection, Collection}
import org.apache.lucene.search.SortField
import org.apache.lucene.{document => ld}

case class TokenizedField[D <: Document[D]](fieldName: String,
                                            indexSupport: IndexSupport[D],
                                            get: D => Option[String])
                                           (implicit val rw: RW[String]) extends LuceneIndexedField[String, D] {
  override protected[lightdb] def createFields(doc: D): List[ld.Field] = get(doc).toList.map { value =>
    new ld.Field(fieldName, value, ld.TextField.TYPE_NOT_STORED)
  }

  override protected[lightdb] def sortType: SortField.Type = SortField.Type.STRING
}