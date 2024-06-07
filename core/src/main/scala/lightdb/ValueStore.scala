package lightdb

import fabric.rw.RW
import lightdb.model.AbstractCollection

case class ValueStore[V, D <: Document[D]](key: String,
                                           createV: D => V,
                                           loadStore: () => Store,
                                           collection: AbstractCollection[D],
                                           distinct: Boolean = true)
                                          (implicit rw: RW[V])
