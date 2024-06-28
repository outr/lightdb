package lightdb.index

import fabric.rw.RW
import lightdb.document.Document

trait Materializable[D <: Document[D], F] {
  def name: String
  def rw: RW[F]
}