package lightdb.lucene.index

import lightdb.index.IndexedField
import lightdb.lucene.LuceneIndexedField
import lightdb.Document
import lightdb.model.Collection
import org.apache.lucene.search.SortField
import org.apache.lucene.{document => ld}

case class TokenizedField[D <: Document[D]](fieldName: String,
                                            collection: Collection[D],
                                            get: D => Option[String]) extends LuceneIndexedField[String, D] {
  override protected[lightdb] def createFields(doc: D): List[ld.Field] = get(doc).toList.map { value =>
    new ld.Field(fieldName, value, ld.TextField.TYPE_NOT_STORED)
  }

  override protected[lightdb] def sortType: SortField.Type = SortField.Type.STRING
}