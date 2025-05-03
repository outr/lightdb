package lightdb.transaction

import lightdb.doc.{Document, DocumentModel}

final case class Tx4[
  D1 <: Document[D1], M1 <: DocumentModel[D1],
  D2 <: Document[D2], M2 <: DocumentModel[D2],
  D3 <: Document[D3], M3 <: DocumentModel[D3],
  D4 <: Document[D4], M4 <: DocumentModel[D4]
](
   ta: Transaction[D1, M1],
   tb: Transaction[D2, M2],
   tc: Transaction[D3, M3],
   td: Transaction[D4, M4]
 )
