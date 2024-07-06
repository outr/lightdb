package lightdb.sql

import fabric.{Json, num, obj}
import fabric.define.DefType
import lightdb.sql.connect.ConnectionManager
import lightdb.{Field, LightDB, Transaction}
import lightdb.doc.DocModel
import lightdb.filter.Filter
import lightdb.store.{Store, StoreManager}
import org.sqlite.SQLiteConfig

import java.nio.file.{Files, Path, StandardCopyOption}
import java.sql.Connection

class SQLiteStore[Doc, Model <: DocModel[Doc]](file: Option[Path]) extends SQLStore[Doc, Model] {
  private val PointRegex = """POINT\((.+) (.+)\)""".r

  private lazy val connection: Connection = {
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

  override protected def initTransaction()(implicit transaction: Transaction[Doc]): Unit = {
    val file = Files.createTempFile("mod_spatialite", ".so")
    val input = getClass.getClassLoader.getResourceAsStream("mod_spatialite.so")
    Files.copy(input, file, StandardCopyOption.REPLACE_EXISTING)
    executeUpdate(s"SELECT load_extension('${file.toAbsolutePath.toString}');")

    super.initTransaction()
  }

  override protected def createTable()(implicit transaction: Transaction[Doc]): Unit = {
    executeUpdate("SELECT InitSpatialMetaData()")

    super.createTable()
  }

  override protected def toJson(value: Any, dt: DefType): Json = if (dt.className.contains("lightdb.spatial.GeoPoint")) {
    value.toString match {
      case PointRegex(longitude, latitude) => obj(
        "latitude" -> num(latitude.toDouble),
        "longitude" -> num(longitude.toDouble)
      )
    }
  } else {
    super.toJson(value, dt)
  }

  override protected def field2Value(field: Field[Doc, _]): String = if (field.rw.definition.className.contains("lightdb.spatial.GeoPoint")) {
    "GeomFromText(?, 4326)"
  } else {
    super.field2Value(field)
  }

  override protected def fieldNamesForDistance(d: Conversion.Distance): List[String] = {
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

  override protected object connectionManager extends ConnectionManager[Doc] {
    override def getConnection(implicit transaction: Transaction[Doc]): Connection = connection

    override def currentConnection(implicit transaction: Transaction[Doc]): Option[Connection] = Some(connection)

    override def releaseConnection(implicit transaction: Transaction[Doc]): Unit = {}

    override def dispose(): Unit = {
      connection.commit()
      connection.close()
    }
  }
}

object SQLiteStore extends StoreManager {
  override def create[Doc, Model <: DocModel[Doc]](db: LightDB, name: String): Store[Doc, Model] =
    new SQLiteStore[Doc, Model](db.directory.map(_.resolve(s"$name.sqlite.db")))
}