import lightdb.doc.Document
import lightdb.id.Id
import lightdb.traverse.{DocumentTraversalBuilder, GraphTraversal}

import scala.language.implicitConversions

package object lightdb {
  /**
   * Convenience method to start a graph traversal directly from the transaction
   */
  def traverseFrom[D <: Document[D]](id: Id[D]): DocumentTraversalBuilder[D] = GraphTraversal.from(id)

  implicit class NumericOps[A](numeric: Numeric[A]) {
    def map[B](to: A => B)(from: B => A): Numeric[B] = new Numeric[B] {
      override def plus(x: B, y: B): B = to(numeric.plus(from(x), from(y)))
      override def minus(x: B, y: B): B = to(numeric.minus(from(x), from(y)))
      override def times(x: B, y: B): B = to(numeric.times(from(x), from(y)))
      override def negate(x: B): B = to(numeric.negate(from(x)))
      override def fromInt(x: Int): B = to(numeric.fromInt(x))
      override def toInt(x: B): Int = numeric.toInt(from(x))
      override def toLong(x: B): Long = numeric.toLong(from(x))
      override def toFloat(x: B): Float = numeric.toFloat(from(x))
      override def toDouble(x: B): Double = numeric.toDouble(from(x))
      override def compare(x: B, y: B): Int = numeric.compare(from(x), from(y))
      override def parseString(str: String): Option[B] = numeric.parseString(str).map(to)
    }
  }
}