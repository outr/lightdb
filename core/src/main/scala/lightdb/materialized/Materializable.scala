package lightdb.materialized

import fabric.rw.RW

trait Materializable[Doc, V] {
  def name: String
  def rw: RW[V]
}
