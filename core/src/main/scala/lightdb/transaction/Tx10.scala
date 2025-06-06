package lightdb.transaction

import lightdb.doc.{Document, DocumentModel}

final case class Tx10[
  D1 <: Document[D1], M1 <: DocumentModel[D1],
  D2 <: Document[D2], M2 <: DocumentModel[D2],
  D3 <: Document[D3], M3 <: DocumentModel[D3],
  D4 <: Document[D4], M4 <: DocumentModel[D4],
  D5 <: Document[D5], M5 <: DocumentModel[D5],
  D6 <: Document[D6], M6 <: DocumentModel[D6],
  D7 <: Document[D7], M7 <: DocumentModel[D7],
  D8 <: Document[D8], M8 <: DocumentModel[D8],
  D9 <: Document[D9], M9 <: DocumentModel[D9],
  D10 <: Document[D10], M10 <: DocumentModel[D10]
](
   ta: Transaction[D1, M1],
   tb: Transaction[D2, M2],
   tc: Transaction[D3, M3],
   td: Transaction[D4, M4],
   te: Transaction[D5, M5],
   tf: Transaction[D6, M6],
   tg: Transaction[D7, M7],
   th: Transaction[D8, M8],
   ti: Transaction[D9, M9],
   tj: Transaction[D10, M10]
 )
