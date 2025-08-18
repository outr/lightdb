package lightdb.spatial

import fabric.{Json, obj, str}

case class MultiLine(lines: List[Line]) extends Geo {
  lazy val center: Point = Geo.center(lines.flatMap(_.points))

  override def toJson: Json = obj(
    "type" -> str("MultiLineString"),
    "coordinates" -> multiLineCoords(this)
  )
}