package lightdb.index

import lightdb.{Collection, Document}
import org.apache.lucene.document.Field
import org.apache.lucene.search.SortField
import org.apache.lucene.{document => ld}

case class DoubleField[D <: Document[D]](fieldName: String,
                                         collection: Collection[D],
                                         get: D => Double) extends IndexedField[Double, D] {
  override protected[lightdb] def createFields(doc: D): List[Field] = List(
    new ld.DoubleField(fieldName, get(doc), Field.Store.NO)
  )

  override protected[lightdb] def sortType: SortField.Type = SortField.Type.DOUBLE
}
