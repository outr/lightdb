package lightdb.facet

import fabric.rw.RW

case class FacetValue(path: List[String])

object FacetValue {
  implicit val rw: RW[FacetValue] = RW.gen

  def apply(path: String*): FacetValue = FacetValue(path.toList)
}