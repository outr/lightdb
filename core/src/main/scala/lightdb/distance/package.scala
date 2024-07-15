package lightdb

import scala.language.implicitConversions

package object distance {
  implicit def int2Extras(i: Int): IntExtras = IntExtras(i)
  implicit def double2Extras(d: Double): DoubleExtras = DoubleExtras(d)
}