package lightdb.sql.geopackage

import fabric.*
import lightdb.spatial.*

import java.nio.file.{Files, Path}
import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.util.Using

/**
 * Writes a GeoPackage (OGC 12-128r19): a single SQLite file carrying one or more vector layers, each
 * with a geometry column and attribute columns, plus the standard metadata tables GIS tools require
 * (`gpkg_spatial_ref_sys`, `gpkg_contents`, `gpkg_geometry_columns`). Geometry is stored in the
 * GeoPackage Binary format (a small header over standard WKB), in WGS 84 (EPSG:4326) - the CRS LightDB's
 * [[Geo]] values are in. The result opens directly in GeoServer, QGIS, ArcGIS, ogr2ogr, and anything
 * else that reads GeoPackage - the modern way to hand a spatial dataset to someone without exposing a
 * database.
 *
 * Pure Scala: the encoding is small and fully specified, so no GeoTools/JTS dependency is pulled in.
 *
 * {{{
 * GeoPackage.write(path, List(
 *   GeoPackage.Layer("wells", GeometryType.Point, columns = List("api" -> ColumnType.Text), rows = ...),
 *   GeoPackage.Layer("units", GeometryType.MultiPolygon, ...)
 * ))
 * }}}
 */
object GeoPackage {
  /** Attribute column types (the SQLite storage classes GeoPackage allows for attributes). */
  enum ColumnType(val sql: String) {
    case Text extends ColumnType("TEXT")
    case Integer extends ColumnType("INTEGER")
    case Real extends ColumnType("REAL")
    case Boolean extends ColumnType("BOOLEAN")
  }

  /** Declared geometry type of a layer (`gpkg_geometry_columns.geometry_type_name`). Use `Geometry`
   *  for a mixed layer. */
  enum GeometryType(val name: String) {
    case Point extends GeometryType("POINT")
    case LineString extends GeometryType("LINESTRING")
    case Polygon extends GeometryType("POLYGON")
    case MultiPoint extends GeometryType("MULTIPOINT")
    case MultiLineString extends GeometryType("MULTILINESTRING")
    case MultiPolygon extends GeometryType("MULTIPOLYGON")
    case GeometryCollection extends GeometryType("GEOMETRYCOLLECTION")
    case Geometry extends GeometryType("GEOMETRY")
  }

  /** One feature: its geometry (None for a feature with attributes but no shape) and attribute values
   *  in the layer's column order (Json Null for a missing value). */
  case class Feature(geometry: Option[Geo], values: List[Json])

  /** A vector layer. `rows` may be a lazy / one-shot iterator so a large layer streams to disk. */
  case class Layer(name: String,
                   geometryType: GeometryType,
                   columns: List[(String, ColumnType)],
                   rows: Iterator[Feature],
                   description: String = "")

  private val Srid = 4326

  /** Writes `layers` to a new GeoPackage at `path` (replacing any existing file). Returns per-layer
   *  feature counts. */
  def write(path: Path, layers: List[Layer]): Map[String, Long] = {
    Files.deleteIfExists(path)
    Option(path.getParent).foreach(Files.createDirectories(_))
    Class.forName("org.sqlite.JDBC")
    Using.resource(DriverManager.getConnection(s"jdbc:sqlite:${path.toAbsolutePath}")) { c =>
      c.setAutoCommit(false)
      createMetadata(c)
      val counts = layers.map(l => l.name -> writeLayer(c, l)).toMap
      c.commit()
      counts
    }
  }

  private def createMetadata(c: Connection): Unit = {
    exec(c, "PRAGMA application_id = 1196444487") // 'GPKG'
    exec(c, "PRAGMA user_version = 10300")        // GeoPackage 1.3.0
    exec(c, """CREATE TABLE gpkg_spatial_ref_sys (
      srs_name TEXT NOT NULL, srs_id INTEGER NOT NULL PRIMARY KEY, organization TEXT NOT NULL,
      organization_coordsys_id INTEGER NOT NULL, definition TEXT NOT NULL, description TEXT)""")
    exec(c, """CREATE TABLE gpkg_contents (
      table_name TEXT NOT NULL PRIMARY KEY, data_type TEXT NOT NULL, identifier TEXT UNIQUE,
      description TEXT DEFAULT '', last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      min_x DOUBLE, min_y DOUBLE, max_x DOUBLE, max_y DOUBLE, srs_id INTEGER,
      CONSTRAINT fk_gc_r_srs_id FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id))""")
    exec(c, """CREATE TABLE gpkg_geometry_columns (
      table_name TEXT NOT NULL, column_name TEXT NOT NULL, geometry_type_name TEXT NOT NULL,
      srs_id INTEGER NOT NULL, z TINYINT NOT NULL, m TINYINT NOT NULL,
      CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name),
      CONSTRAINT uk_gc_table_name UNIQUE (table_name),
      CONSTRAINT fk_gc_tn FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name),
      CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id))""")
    // The three SRS entries the spec requires, plus WGS 84 which is the one we write in.
    exec(c, """INSERT INTO gpkg_spatial_ref_sys VALUES
      ('Undefined cartesian SRS', -1, 'NONE', -1, 'undefined', 'undefined cartesian coordinate reference system'),
      ('Undefined geographic SRS', 0, 'NONE', 0, 'undefined', 'undefined geographic coordinate reference system'),
      ('WGS 84 geodetic', 4326, 'EPSG', 4326,
       'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]',
       'longitude/latitude coordinates in decimal degrees on the WGS 84 spheroid')""")
  }

  private def writeLayer(c: Connection, layer: Layer): Long = {
    val t = quote(layer.name)
    val attrCols = layer.columns.map { case (n, ct) => s"${quote(n)} ${ct.sql}" }
    exec(c, s"CREATE TABLE $t (fid INTEGER PRIMARY KEY AUTOINCREMENT, geom BLOB${attrCols.map(", " + _).mkString})")
    val placeholders = ("?" :: layer.columns.map(_ => "?")).mkString(", ")
    val insert = c.prepareStatement(
      s"INSERT INTO $t (geom${layer.columns.map(c => ", " + quote(c._1)).mkString}) VALUES ($placeholders)")
    var count = 0L
    var minX = Double.MaxValue; var minY = Double.MaxValue; var maxX = Double.MinValue; var maxY = Double.MinValue
    Using.resource(insert) { ps =>
      layer.rows.foreach { f =>
        f.geometry match {
          case Some(g) =>
            ps.setBytes(1, GeoPackageBinary.encode(g, Srid))
            val e = envelope(g)
            minX = math.min(minX, e._1); minY = math.min(minY, e._2); maxX = math.max(maxX, e._3); maxY = math.max(maxY, e._4)
          case None => ps.setNull(1, java.sql.Types.BLOB)
        }
        f.values.zipWithIndex.foreach { case (v, i) => bind(ps, i + 2, v, layer.columns(i)._2) }
        ps.addBatch()
        count += 1
        if (count % 5000 == 0) ps.executeBatch()
      }
      ps.executeBatch()
    }
    val hasExtent = count > 0 && minX != Double.MaxValue
    val ext = if (hasExtent) List(minX, minY, maxX, maxY).map(d => java.lang.Double.valueOf(d)) else List(null, null, null, null)
    Using.resource(c.prepareStatement(
      "INSERT INTO gpkg_contents (table_name, data_type, identifier, description, min_x, min_y, max_x, max_y, srs_id) VALUES (?, 'features', ?, ?, ?, ?, ?, ?, ?)")) { ps =>
      ps.setString(1, layer.name); ps.setString(2, layer.name); ps.setString(3, layer.description)
      ext.zipWithIndex.foreach { case (v, i) => if (v == null) ps.setNull(4 + i, java.sql.Types.DOUBLE) else ps.setDouble(4 + i, v.doubleValue()) }
      ps.setInt(8, Srid)
      ps.executeUpdate()
    }
    Using.resource(c.prepareStatement(
      "INSERT INTO gpkg_geometry_columns VALUES (?, 'geom', ?, ?, 0, 0)")) { ps =>
      ps.setString(1, layer.name); ps.setString(2, layer.geometryType.name); ps.setInt(3, Srid)
      ps.executeUpdate()
    }
    count
  }

  private def bind(ps: PreparedStatement, idx: Int, v: Json, ct: ColumnType): Unit = v match {
    case Null => ps.setNull(idx, java.sql.Types.NULL)
    case Str(s, _) => ps.setString(idx, s)
    case NumInt(l, _) => ps.setLong(idx, l)
    case NumDec(d, _) => ps.setDouble(idx, d.toDouble)
    case Bool(b, _) => ps.setBoolean(idx, b)
    case other => ps.setString(idx, fabric.io.JsonFormatter.Compact(other)) // arrays/objects as JSON text
  }

  /** (minX, minY, maxX, maxY) in lon/lat. */
  private def envelope(g: Geo): (Double, Double, Double, Double) = {
    val pts = GeoPackageBinary.pointsOf(g)
    (pts.map(_.longitude).min, pts.map(_.latitude).min, pts.map(_.longitude).max, pts.map(_.latitude).max)
  }

  private def exec(c: Connection, sql: String): Unit = Using.resource(c.createStatement())(_.execute(sql))
  private def quote(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""
}
