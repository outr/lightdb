package lightdb.spatial

import fabric._
import fabric.io.JsonFormatter
import fabric.rw._

sealed trait Geo {
  def center: Geo.Point
}

object Geo {
  implicit lazy val pRW: RW[Point] = RW.gen[Point]
    .withPreWrite(_.merge(obj("type" -> "Point")))
    .withPostRead((_, json) => json.merge(obj("type" -> "Point")))
  private implicit lazy val mpRW: RW[MultiPoint] = RW.gen
  private implicit lazy val lsRW: RW[Line] = RW.gen
  private implicit lazy val mlsRW: RW[MultiLine] = RW.gen
  private implicit lazy val plyRW: RW[Polygon] = RW.gen
  private implicit lazy val mplyRW: RW[MultiPolygon] = RW.gen

  implicit val rw: RW[Geo] = RW.poly[Geo](className = Some("lightdb.spatial.Geo"))(
    pRW, mpRW, lsRW, mlsRW, plyRW, mplyRW //, RW.gen[GeometryCollection]
  )

  private lazy val PointStringRegex = """POINT\((.+) (.+)\)""".r
  private lazy val PolygonRegex = """POLYGON\(\((.+)\)\)""".r
  private lazy val MultiPolygonRegex = """MULTIPOLYGON\(\(\((.+)\)\)\)""".r

  def parseString(s: String): Geo = s match {
    case PointStringRegex(lon, lat) => Geo.Point(
      latitude = lat.toDouble, longitude = lon.toDouble
    )
    case PolygonRegex(p) => parsePolyString(p)
    case MultiPolygonRegex(p) => MultiPolygon(p.split("""\)\),\(\(""").toList.map(parsePolyString))
    case _ => throw new RuntimeException(s"Unsupported GeoString: $s")
  }

  private def parsePolyString(s: String): Geo.Polygon = Geo.Polygon(s.split(',').toList.map { p =>
    val v = p.split(' ').map(_.trim)
    Geo.Point(
      latitude = v(1).toDouble, longitude = v(0).toDouble
    )
  })

  def parseMulti(json: Json): List[Geo] = parse(json) match {
    case GeometryCollection(geometries) => geometries
    case geo => List(geo)
  }

  def parse(json: Json): Geo = json("type").asString match {
    case "Point" =>
      val v = json("coordinates").asVector
      Geo.Point(latitude = v(1).asDouble, longitude = v(0).asDouble)
    case "LineString" => Line(
      json("coordinates").asVector.toList.map { p =>
        val v = p.asVector
        Geo.Point(latitude = v(1).asDouble, longitude = v(0).asDouble)
      }
    )
    case "Polygon" => Polygon(
      json("coordinates").asVector.head.asVector.toList.map { p =>
        val v = p.asVector
        Geo.Point(latitude = v(1).asDouble, longitude = v(0).asDouble)
      }
    )
    case "MultiPolygon" => MultiPolygon(
      json("coordinates").asVector.toList.map { p =>
        p.asVector.head.asVector.toList.map(_.asVector.toList).map { v =>
          Geo.Point(latitude = v(1).asDouble, longitude = v(0).asDouble)
        }
      }.map(list => Polygon(list))
    )
    case "GeometryCollection" => GeometryCollection(
      json("geometries").asVector.toList.map(parse)
    ).normalized
    case t => throw new RuntimeException(s"Unsupported GeoJson type $t:\n${JsonFormatter.Default(json)}")
  }

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
  case class GeometryCollection(geometries: List[Geo]) extends Geo {
    lazy val center: Point = Geo.center(geometries.map(_.center))

    lazy val normalized: Geo = geometries match {
      case geo :: Nil => geo
      case _ => this
    }
  }
}