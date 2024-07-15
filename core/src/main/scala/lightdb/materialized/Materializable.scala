package lightdb.materialized

import fabric.rw._

trait Materializable[Doc, V] {
  def name: String
  def rw: RW[V]
}
