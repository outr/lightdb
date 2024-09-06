package lightdb.spatial

import fabric.rw._

sealed trait Geo {
  def center: Geo.Point
}

object Geo {
  implicit val pRW: RW[Point] = RW.gen
  private implicit val mpRW: RW[MultiPoint] = RW.gen
  private implicit val lsRW: RW[Line] = RW.gen
  private implicit val mlsRW: RW[MultiLine] = RW.gen
  private implicit val plyRW: RW[Polygon] = RW.gen
  private implicit val mplyRW: RW[MultiPolygon] = RW.gen

  implicit val rw: RW[Geo] = RW.poly[Geo](className = Some("lightdb.spatial.Geo"))(
    pRW, mpRW, lsRW, mlsRW, plyRW, mplyRW
  )

  def min(points: List[Point]): Point = {
    val latitude = points.map(_.latitude).min
    val longitude = points.map(_.longitude).min
    Point(latitude, longitude)
  }

  def max(points: List[Point]): Point = {
    val latitude = points.map(_.latitude).max
    val longitude = points.map(_.longitude).max
    Point(latitude, longitude)
  }

  def center(points: List[Point]): Point = {
    val min = this.min(points)
    val max = this.max(points)
    val latitude = min.latitude + (max.latitude - min.latitude)
    val longitude = min.longitude + (max.longitude - min.longitude)
    Point(latitude, longitude)
  }

  case class Point(latitude: Double, longitude: Double) extends Geo {
    override def center: Point = this
  }
  case class MultiPoint(points: List[Point]) extends Geo {
    lazy val center: Point = Geo.center(points)
  }
  case class Line(points: List[Point]) extends Geo {
    lazy val center: Point = Geo.center(points)
  }
  case class MultiLine(lines: List[Line]) extends Geo {
    lazy val center: Point = Geo.center(lines.flatMap(_.points))
  }
  case class Polygon(points: List[Point]) extends Geo {
    lazy val center: Point = Geo.center(points)
  }
  object Polygon {
    def lonLat(points: Double*): Polygon = Polygon(points.grouped(2).map { p =>
      Point(p.last, p.head)
    }.toList)
  }
  case class MultiPolygon(polygons: List[Polygon]) extends Geo {
    lazy val center: Point = Geo.center(polygons.flatMap(_.points))
  }
}