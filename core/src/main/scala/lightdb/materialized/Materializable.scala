package lightdb.materialized

import fabric.rw.*

trait Materializable[Doc, V] {
  def name: String
  def rw: RW[V]
}
