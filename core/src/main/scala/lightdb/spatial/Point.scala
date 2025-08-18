package lightdb.spatial

import fabric.rw.RW
import fabric.{Json, obj, str}

case class Point(latitude: Double, longitude: Double) extends Geo {
  override def center: Point = this

  def fixed: Point = if (latitude < -90.0 || latitude > 90.0) {
    Point(longitude, latitude)
  } else {
    this
  }

  override def toJson: Json = obj(
    "type" -> str("Point"),
    "coordinates" -> coord(this)
  )
}

object Point {
  implicit val rw: RW[Point] = Geo.rw.asInstanceOf[RW[Point]]
}