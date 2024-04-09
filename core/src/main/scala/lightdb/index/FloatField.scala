package lightdb.index

import lightdb.{Collection, Document}
import org.apache.lucene.document.Field
import org.apache.lucene.search.SortField
import org.apache.lucene.{document => ld}

case class FloatField[D <: Document[D]](fieldName: String,
                                        collection: Collection[D],
                                        get: D => Float) extends IndexedField[Float, D] {
  override protected[lightdb] def createFields(doc: D): List[Field] = List(
    new ld.FloatField(fieldName, get(doc), Field.Store.NO)
  )

  override protected[lightdb] def sortType: SortField.Type = SortField.Type.FLOAT
}
