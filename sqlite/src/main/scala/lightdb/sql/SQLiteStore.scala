package lightdb.sql

import fabric._
import fabric.rw._
import lightdb.sql.connect.{ConnectionManager, DBCPConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.{Field, LightDB}
import lightdb.doc.{Document, DocumentModel}
import lightdb.filter.Filter
import lightdb.store.{Conversion, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import org.sqlite.SQLiteConfig

import java.nio.file.{Files, Path, StandardCopyOption}
import java.sql.Connection

class SQLiteStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](file: Option[Path], val storeMode: StoreMode) extends SQLStore[Doc, Model] {
  private val PointRegex = """POINT\((.+) (.+)\)""".r

  override protected lazy val connectionManager: ConnectionManager = {
    val connection: Connection = {
      val path = file match {
        case Some(f) =>
          val file = f.toFile
          Option(file.getParentFile).foreach(_.mkdirs())
          file.getCanonicalPath
        case None => ":memory:"
      }

      val config = new SQLiteConfig
      config.enableLoadExtension(true)
      val c = config.createConnection(s"jdbc:sqlite:$path")
      c.setAutoCommit(false)
      c
    }
    SingleConnectionManager(connection)
  }

  override protected def initTransaction()(implicit transaction: Transaction[Doc]): Unit = {
    val file = Files.createTempFile("mod_spatialite", ".so")
    val input = getClass.getClassLoader.getResourceAsStream("mod_spatialite.so")
    Files.copy(input, file, StandardCopyOption.REPLACE_EXISTING)
    val path = file.toAbsolutePath.toString match {
      case s => s.substring(0, s.length - 3)
    }
    scribe.info(s"Copying to ${file.toFile.getCanonicalPath} loading: $path")
    executeUpdate(s"SELECT load_extension('$path');")

    super.initTransaction()
  }

  override protected def createTable()(implicit transaction: Transaction[Doc]): Unit = {
    executeUpdate("SELECT InitSpatialMetaData()")

    super.createTable()
  }

  override protected def tables(connection: Connection): Set[String] = {
    val ps = connection.prepareStatement(s"SELECT name FROM sqlite_master WHERE type = 'table';")
    try {
      val rs = ps.executeQuery()
      try {
        var set = Set.empty[String]
        while (rs.next()) {
          set += rs.getString("name").toLowerCase
        }
        set
      } finally {
        rs.close()
      }
    } finally {
      ps.close()
    }
  }

  override protected def toJson(value: Any, rw: RW[_]): Json = if (rw.definition.className.contains("lightdb.spatial.GeoPoint")) {
    value.toString match {
      case PointRegex(longitude, latitude) => obj(
        "latitude" -> num(latitude.toDouble),
        "longitude" -> num(longitude.toDouble)
      )
    }
  } else {
    super.toJson(value, rw)
  }

  override protected def field2Value(field: Field[Doc, _]): String = if (field.rw.definition.className.contains("lightdb.spatial.GeoPoint")) {
    "GeomFromText(?, 4326)"
  } else {
    super.field2Value(field)
  }

  override protected def fieldNamesForDistance(d: Conversion.Distance[Doc]): List[String] = {
    s"AsText(${d.field.name}) AS ${d.field.name}" ::
    s"ST_Distance(GeomFromText('POINT(${d.from.longitude} ${d.from.latitude})', 4326), ${d.field.name}, true) AS ${d.field.name}Distance" ::
    collection.model.fields.filterNot(_ eq d.field).map(_.name)
  }

  override protected def distanceFilter(f: Filter.Distance[Doc]): SQLPart =
    SQLPart(s"ST_Distance(${f.field.name}, GeomFromText(?, 4326), true) <= ?", List(SQLArg.GeoPointArg(f.from), SQLArg.DoubleArg(f.radius.m)))

  override protected def addColumn(field: Field[Doc, _])(implicit transaction: Transaction[Doc]): Unit = {
    if (field.rw.definition.className.contains("lightdb.spatial.GeoPoint")) {
      executeUpdate(s"SELECT AddGeometryColumn('${collection.name}', '${field.name}', 4326, 'POINT', 'XY');")
    } else {
      super.addColumn(field)
    }
  }

  override def size: Long = file.map(_.toFile.length()).getOrElse(0L)
}

object SQLiteStore extends StoreManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB, name: String, storeMode: StoreMode): Store[Doc, Model] =
    new SQLiteStore[Doc, Model](db.directory.map(_.resolve(s"$name.sqlite.db")), storeMode)
}