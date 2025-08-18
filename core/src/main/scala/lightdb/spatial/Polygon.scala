package lightdb.spatial

import fabric.{Json, obj, str}

case class Polygon(points: List[Point]) extends Geo {
  lazy val center: Point = Geo.center(points)

  override def toJson: Json = obj(
    "type" -> str("Polygon"),
    "coordinates" -> polygonCoords(this)
  )
}

object Polygon {
  def lonLat(points: Double*): Polygon = Polygon(points.grouped(2).map { p =>
    Point(p.last, p.head)
  }.toList)
}