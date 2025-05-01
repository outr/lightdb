package lightdb.transaction

import lightdb.doc.{Document, DocumentModel}

final case class Tx3[
  D1 <: Document[D1], M1 <: DocumentModel[D1],
  D2 <: Document[D2], M2 <: DocumentModel[D2],
  D3 <: Document[D3], M3 <: DocumentModel[D3]
](
   ta: Transaction[D1, M1],
   tb: Transaction[D2, M2],
   tc: Transaction[D3, M3]
 )
