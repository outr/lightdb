package lightdb.spatial

import fabric.*
import fabric.io.JsonFormatter
import fabric.rw.*

trait Geo {
  def center: Point

  def toJson: Json

  protected def coord(p: Point): Json =
    arr(num(p.longitude), num(p.latitude))

  def polygons: List[Polygon] = this match {
    case p: Polygon => List(p)
    case MultiPolygon(polygons) => polygons
    case Line(points) if points.size >= 3 => List(Polygon(points ++ points.reverse.tail))
    case Line(_) => Nil // Ignore lines that cannot form a valid polygon
    case geo => throw new RuntimeException(s"Unsupported spatial: $geo")
  }

  protected def ensureClosed(points: List[Point]): List[Point] =
    points match {
      case Nil => Nil
      case ps if ps.head == ps.last => ps
      case ps => ps :+ ps.head
    }

  protected def ring(points: List[Point]): Json =
    arr(ensureClosed(points).map(coord): _*)

  protected def polygonCoords(poly: Polygon): Json = {
    arr(ring(poly.points))
  }

  protected def lineCoords(line: Line): Json =
    arr(line.points.map(coord): _*)

  protected def multiLineCoords(ml: MultiLine): Json =
    arr(ml.lines.map(l => lineCoords(l)): _*)

  protected def multiPointCoords(mp: MultiPoint): Json =
    arr(mp.points.map(coord): _*)

  protected def multiPolygonCoords(mp: MultiPolygon): Json =
    arr(mp.polygons.map(p => polygonCoords(p)): _*)
}

object Geo {
  implicit val rw: RW[Geo] = RW.from[Geo](
    r = geo => geo.toJson,
    w = json => parse(json),
    d = fabric.define.Definition(fabric.define.DefType.Json, className = Some("lightdb.spatial.Geo"))
  )

  private lazy val PointStringRegex = """(?i)^\s*POINT\s*\(\s*([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s*\)\s*$""".r
  private lazy val PolygonRegex = """(?i)^\s*POLYGON\s*\(\s*\((.+)\)\s*(?:,\s*\(.+\)\s*)*\)\s*$""".r

  def parseString(s: String): Geo = s.trim match {
    case PointStringRegex(lon, lat) =>
      Point(latitude = lat.toDouble, longitude = lon.toDouble)

    case PolygonRegex(firstRing) =>
      parsePolyRing(firstRing)

    case _ if s.trim.toUpperCase.startsWith("MULTIPOLYGON") =>
      val inner = s.trim
        .stripPrefix("MULTIPOLYGON").dropWhile(_ != '(').drop(1) // drop up to first '('
        .reverse.dropWhile(_ != ')').drop(1).reverse // drop trailing ')'
        .trim
      val polys = inner.split("""\)\s*\)\s*,\s*\(\s*\(""") // split between polygons
      val polygons = polys.toList.map { poly =>
        val firstRing = poly.split("""\)\s*,\s*\(""", 2).head
        parsePolyRing(firstRing)
      }
      MultiPolygon(polygons)

    case _ =>
      throw new RuntimeException(s"Unsupported GeoString: $s")
  }

  private def parsePolyRing(ring: String): Polygon = {
    val cleaned = ring.replace("(", "").replace(")", "").trim
    val pts = cleaned.split("""\s*,\s*""").toList.map { p =>
      val parts = p.trim.split("""\s+""")
      if parts.length < 2 then
        throw new RuntimeException(s"Bad coordinate: '$p' in ring '$ring'")
      val lon = parts(0).toDouble
      val lat = parts(1).toDouble
      Point(latitude = lat, longitude = lon)
    }
    Polygon(pts)
  }

  def parseMulti(json: Json): List[Geo] = parse(json) match {
    case GeometryCollection(geometries) => geometries
    case geo => List(geo)
  }

  def parse(json: Json): Geo = json("type").asString match {
    case "Point" =>
      json.get("coordinates") match {
        case Some(coordinates) =>
          val v = coordinates.asVector.map(_.asDouble)
          Point(latitude = v(1), longitude = v(0)).fixed
        case None => Point(latitude = json("latitude").asDouble, longitude = json("longitude").asDouble)
      }
    case "LineString" => Line(
      json("coordinates").asVector.toList.map { p =>
        val v = p.asVector
        Point(latitude = v(1).asDouble, longitude = v(0).asDouble).fixed
      }
    )
    case "Polygon" => Polygon(
      json("coordinates").asVector.head.asVector.toList.map { p =>
        val v = p.asVector
        Point(latitude = v(1).asDouble, longitude = v(0).asDouble).fixed
      }
    )
    case "MultiPolygon" => MultiPolygon(
      json("coordinates").asVector.toList.map { p =>
        p.asVector.head.asVector.toList.map(_.asVector.toList).map { v =>
          Point(latitude = v(1).asDouble, longitude = v(0).asDouble).fixed
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
    val latitude = min.latitude + (max.latitude - min.latitude) / 2.0
    val longitude = min.longitude + (max.longitude - min.longitude) / 2.0
    Point(latitude, longitude)
  }
}