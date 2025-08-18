package lightdb.spatial

import fabric.{Json, obj, str}

case class Line(points: List[Point]) extends Geo {
  lazy val center: Point = Geo.center(points)

  override def toJson: Json = obj(
    "type" -> str("LineString"),
    "coordinates" -> lineCoords(this)
  )
}