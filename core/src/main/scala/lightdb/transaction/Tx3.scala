package lightdb.transaction

import lightdb.doc.Document

final case class Tx3[A <: Document[A], B <: Document[B], C <: Document[C]](ta: Transaction[A],
                                                                           tb: Transaction[B],
                                                                           tc: Transaction[C])
