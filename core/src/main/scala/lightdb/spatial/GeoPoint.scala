package lightdb.spatial

import fabric.rw.RW

case class GeoPoint(latitude: Double, longitude: Double)

object GeoPoint {
  implicit val rw: RW[GeoPoint] = RW.gen
}
