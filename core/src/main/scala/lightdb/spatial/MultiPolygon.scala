package lightdb.spatial

import fabric.{Json, obj, str}

case class MultiPolygon(polygons: List[Polygon]) extends Geo {
  lazy val center: Point = Geo.center(polygons.flatMap(_.points))

  override def toJson: Json = obj(
    "type" -> str("MultiPolygon"),
    "coordinates" -> multiPolygonCoords(this)
  )
}