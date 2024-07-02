package lightdb.index

import fabric.rw.RW
import lightdb.LightDB
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.query.Query

class IndexedCollection[D <: Document[D], M <: DocumentModel[D]](name: String,
                                                                 model: M,
                                                                 val indexer: Indexer[D, M],
                                                                 db: LightDB)
                                                                (implicit rw: RW[D]) extends Collection[D, M](name, model, db) {
  def query: Query[D, M] = Query(indexer, this)
}