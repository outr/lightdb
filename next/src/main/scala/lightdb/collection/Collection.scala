package lightdb.collection

import lightdb.LightDB
import lightdb.document.Document

trait Collection[D <: Document[D]] {
  def db: LightDB
}