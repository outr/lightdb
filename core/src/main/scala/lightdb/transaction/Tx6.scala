package lightdb.transaction

import lightdb.doc.Document

final case class Tx6[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F]](
                                                                                                                                  ta: Transaction[A],
                                                                                                                                  tb: Transaction[B],
                                                                                                                                  tc: Transaction[C],
                                                                                                                                  td: Transaction[D],
                                                                                                                                  te: Transaction[E],
                                                                                                                                  tf: Transaction[F]
                                                                                                                                )
