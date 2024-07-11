package lightdb

import lightdb.doc.Document
import lightdb.transaction.Transaction

import scala.language.implicitConversions

package object sql {
  implicit def transaction2Impl[Doc <: Document[Doc]](transaction: Transaction[Doc]): SQLTransaction[Doc] =
    transaction.asInstanceOf[SQLTransaction[Doc]]
}
