package lightdb.spatial

import fabric.{Json, arr, obj, str}

case class GeometryCollection(geometries: List[Geo]) extends Geo {
  lazy val center: Point = Geo.center(geometries.map(_.center))

  lazy val normalized: Geo = geometries match {
    case geo :: Nil => geo
    case _ => this
  }

  override def toJson: Json = obj(
    "type" -> str("GeometryCollection"),
    "geometries" -> arr(geometries.map(_.toJson): _*)
  )
}