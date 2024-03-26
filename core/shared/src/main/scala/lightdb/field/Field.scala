package lightdb.field

import fabric.rw.RW
import lightdb.{Document, ObjectMapping}

case class Field[D <: Document[D], F](name: String,
                                      getter: D => F,
                                      mapping: ObjectMapping[D],
                                      stored: Boolean = true)
                                     (implicit val rw: RW[F])