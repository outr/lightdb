package lightdb.spatial

import fabric._
import fabric.define.DefType
import fabric.io.JsonFormatter
import fabric.rw._

sealed trait Geo {
  import Geo._

  def center: Geo.Point

  def toJson: Json

  protected def coord(p: Point): Json =
    arr(num(p.longitude), num(p.latitude))

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
    d = {
      import fabric._
      import fabric.define.DefType
      import fabric.define.DefType.{Arr, Dec, Enum, Obj, Json, Poly}

      val position: DefType = Arr(Dec)
      val ring: DefType = Arr(position)
      val polygonCoords: DefType = Arr(ring)
      val multiPointCoords: DefType = Arr(position)
      val lineCoords: DefType = Arr(position)
      val multiLineCoords: DefType = Arr(lineCoords)
      val multiPolygonCoords: DefType = Arr(polygonCoords)

      def typeIs(name: String): DefType =
        Enum(values = List(str(name)), className = Some("java.lang.String"))

      val pointDef: DefType =
        Obj(className = None,
          "type" -> typeIs("Point"),
          "coordinates" -> position
        )

      val multiPointDef: DefType =
        Obj(className = None,
          "type" -> typeIs("MultiPoint"),
          "coordinates" -> multiPointCoords
        )

      val lineStringDef: DefType =
        Obj(className = None,
          "type" -> typeIs("LineString"),
          "coordinates" -> lineCoords
        )

      val multiLineStringDef: DefType =
        Obj(className = None,
          "type" -> typeIs("MultiLineString"),
          "coordinates" -> multiLineCoords
        )

      val polygonDef: DefType =
        Obj(className = None,
          "type" -> typeIs("Polygon"),
          "coordinates" -> polygonCoords
        )

      val multiPolygonDef: DefType =
        Obj(className = None,
          "type" -> typeIs("MultiPolygon"),
          "coordinates" -> multiPolygonCoords
        )

      val geometryCollectionDef: DefType =
        Obj(className = None,
          "type" -> typeIs("GeometryCollection"),
          "geometries" -> Arr(Json) // array of geometries; each will be one of the poly variants at runtime
        )

      Poly(
        values = Map(
          "Point" -> pointDef,
          "MultiPoint" -> multiPointDef,
          "LineString" -> lineStringDef,
          "MultiLineString" -> multiLineStringDef,
          "Polygon" -> polygonDef,
          "MultiPolygon" -> multiPolygonDef,
          "GeometryCollection" -> geometryCollectionDef
        ),
        className = Some("lightdb.spatial.Geo")
      )
    }
  )

  private lazy val PointStringRegex = """(?i)^\s*POINT\s*\(\s*([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)\s*\)\s*$""".r
  private lazy val PolygonRegex = """(?i)^\s*POLYGON\s*\(\s*\((.+)\)\s*(?:,\s*\(.+\)\s*)*\)\s*$""".r

  def parseString(s: String): Geo = s.trim match {
    case PointStringRegex(lon, lat) =>
      Geo.Point(latitude = lat.toDouble, longitude = lon.toDouble)

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
      Geo.MultiPolygon(polygons)

    case _ =>
      throw new RuntimeException(s"Unsupported GeoString: $s")
  }

  private def parsePolyRing(ring: String): Geo.Polygon = {
    val cleaned = ring.replace("(", "").replace(")", "").trim
    val pts = cleaned.split("""\s*,\s*""").toList.map { p =>
      val parts = p.trim.split("""\s+""")
      if (parts.length < 2)
        throw new RuntimeException(s"Bad coordinate: '$p' in ring '$ring'")
      val lon = parts(0).toDouble
      val lat = parts(1).toDouble
      Geo.Point(latitude = lat, longitude = lon)
    }
    Geo.Polygon(pts)
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
          Geo.Point(latitude = v(1), longitude = v(0)).fixed
        case None => Geo.Point(latitude = json("latitude").asDouble, longitude = json("longitude").asDouble)
      }
    case "LineString" => Line(
      json("coordinates").asVector.toList.map { p =>
        val v = p.asVector
        Geo.Point(latitude = v(1).asDouble, longitude = v(0).asDouble).fixed
      }
    )
    case "Polygon" => Polygon(
      json("coordinates").asVector.head.asVector.toList.map { p =>
        val v = p.asVector
        Geo.Point(latitude = v(1).asDouble, longitude = v(0).asDouble).fixed
      }
    )
    case "MultiPolygon" => MultiPolygon(
      json("coordinates").asVector.toList.map { p =>
        p.asVector.head.asVector.toList.map(_.asVector.toList).map { v =>
          Geo.Point(latitude = v(1).asDouble, longitude = v(0).asDouble).fixed
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

  case class MultiPoint(points: List[Point]) extends Geo {
    lazy val center: Point = Geo.center(points)

    override def toJson: Json = obj(
      "type" -> str("MultiPoint"),
      "coordinates" -> multiPointCoords(this)
    )
  }

  case class Line(points: List[Point]) extends Geo {
    lazy val center: Point = Geo.center(points)

    override def toJson: Json = obj(
      "type" -> str("LineString"),
      "coordinates" -> lineCoords(this)
    )
  }

  case class MultiLine(lines: List[Line]) extends Geo {
    lazy val center: Point = Geo.center(lines.flatMap(_.points))

    override def toJson: Json = obj(
      "type" -> str("MultiLineString"),
      "coordinates" -> multiLineCoords(this)
    )
  }

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

  case class MultiPolygon(polygons: List[Polygon]) extends Geo {
    lazy val center: Point = Geo.center(polygons.flatMap(_.points))

    override def toJson: Json = obj(
      "type" -> str("MultiPolygon"),
      "coordinates" -> multiPolygonCoords(this)
    )
  }

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
}