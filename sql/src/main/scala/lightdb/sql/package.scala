package lightdb

import scala.language.implicitConversions

package object sql {
  implicit def transaction2Impl[Doc](transaction: Transaction[Doc]): SQLTransaction[Doc] =
    transaction.asInstanceOf[SQLTransaction[Doc]]
}
