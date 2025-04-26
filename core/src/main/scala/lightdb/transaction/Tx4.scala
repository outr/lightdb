package lightdb.transaction

import lightdb.doc.Document

final case class Tx4[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D]](ta: Transaction[A],
                                                                                             tb: Transaction[B],
                                                                                             tc: Transaction[C],
                                                                                             td: Transaction[D])
