package lightdb.aggregate

import fabric.rw.RW
import lightdb.Document

case class AggregateFunction[F, D <: Document[D]](name: String, fieldName: String, `type`: AggregateType, rw: RW[F])