package lightdb.index

import lightdb.document.{Document, DocumentModel}

trait IndexerManager {
  def create[D <: Document[D], M <: DocumentModel[D]](): Indexer[D, M]
}
