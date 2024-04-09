package lightdb.index

import lightdb.{Collection, Document}
import org.apache.lucene.search.SortField
import org.apache.lucene.{document => ld}

case class TokenizedField[D <: Document[D]](fieldName: String,
                                            collection: Collection[D],
                                            get: D => String) extends IndexedField[String, D] {
  override protected[lightdb] def createFields(doc: D): List[ld.Field] = List(
    new ld.Field(fieldName, get(doc), ld.TextField.TYPE_NOT_STORED)
  )

  override protected[lightdb] def sortType: SortField.Type = SortField.Type.STRING
}