package lightdb.spatial

import fabric.{Json, obj, str}

case class MultiPoint(points: List[Point]) extends Geo {
  lazy val center: Point = Geo.center(points)

  override def toJson: Json = obj(
    "type" -> str("MultiPoint"),
    "coordinates" -> multiPointCoords(this)
  )
}