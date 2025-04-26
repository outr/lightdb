package lightdb.transaction

import lightdb.doc.Document

final case class Tx5[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E]](
                                                                                                                ta: Transaction[A],
                                                                                                                tb: Transaction[B],
                                                                                                                tc: Transaction[C],
                                                                                                                td: Transaction[D],
                                                                                                                te: Transaction[E]
                                                                                                              )
