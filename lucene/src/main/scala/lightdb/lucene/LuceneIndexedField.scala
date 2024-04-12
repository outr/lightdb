package lightdb.lucene

import lightdb.Document
import lightdb.index.IndexedField
import org.apache.lucene.search.SortField
import org.apache.lucene.{document => ld}

trait LuceneIndexedField[F, D <: Document[D]] extends IndexedField[F, D] {
  protected[lightdb] def createFields(doc: D): List[ld.Field]

  protected[lightdb] def sortType: SortField.Type
}
