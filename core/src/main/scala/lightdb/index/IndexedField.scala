package lightdb.index

import lightdb.{Collection, Document}
import org.apache.lucene.search.SortField
import org.apache.lucene.{document => ld}

trait IndexedField[F, D <: Document[D]] {
  def fieldName: String
  def collection: Collection[D]
  def get: D => F

  collection.index.register(this)

  protected[lightdb] def createFields(doc: D): List[ld.Field]
  protected[lightdb] def sortType: SortField.Type
}