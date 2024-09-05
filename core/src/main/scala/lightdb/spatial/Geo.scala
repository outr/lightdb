package lightdb.spatial

import fabric.rw.RW

sealed trait Geo

object Geo {
  implicit val pRW: RW[Point] = RW.gen
  implicit val mpRW: RW[MultiPoint] = RW.gen
  implicit val lsRW: RW[LineString] = RW.gen
  implicit val mlsRW: RW[MultiLineString] = RW.gen
  implicit val plyRW: RW[Polygon] = RW.gen
  implicit val mplyRW: RW[MultiPolygon] = RW.gen

  implicit val rw: RW[Geo] = RW.poly[Geo]()(
    pRW, mpRW, lsRW, mlsRW, plyRW, mplyRW
  )

  case class Point(latitude: Double, longitude: Double) extends Geo
  case class MultiPoint(points: List[Point]) extends Geo
  case class LineString(points: List[Point]) extends Geo
  case class MultiLineString(lines: List[LineString]) extends Geo
  case class Polygon(points: List[Point]) extends Geo
  case class MultiPolygon(polygons: List[Polygon]) extends Geo
}