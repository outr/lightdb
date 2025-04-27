package lightdb.transaction

import lightdb.doc.Document

final case class Tx2[A <: Document[A], B <: Document[B]](ta: Transaction[A],
                                                         tb: Transaction[B])
