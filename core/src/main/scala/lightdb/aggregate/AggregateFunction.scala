package lightdb.aggregate

import fabric.rw.RW
import lightdb.Document
import lightdb.index.Index

case class AggregateFunction[T, F, D <: Document[D]](name: String, index: Index[F, D], `type`: AggregateType)
                                                    (implicit val rw: RW[T])