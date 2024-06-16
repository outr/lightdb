package lightdb.transaction

import lightdb.collection.Collection
import lightdb.document.Document

case class Transaction[D <: Document[D]](collection: Collection[D])