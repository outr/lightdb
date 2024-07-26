package lightdb.sql

import fabric._
import fabric.rw._
import lightdb.sql.connect.{ConnectionManager, DBCPConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.{Field, LightDB}
import lightdb.doc.{Document, DocumentModel}
import lightdb.filter.Filter
import lightdb.store.{Conversion, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import org.sqlite.{SQLiteConfig, SQLiteOpenMode}
import org.sqlite.SQLiteConfig.{JournalMode, LockingMode, SynchronousMode, TransactionMode}

import java.io.File
import java.nio.file.{Files, Path, StandardCopyOption}
import java.sql.Connection

class SQLiteStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](val connectionManager: ConnectionManager,
                                                                     val connectionShared: Boolean,
                                                                     val storeMode: StoreMode) extends SQLStore[Doc, Model] {
  private val PointRegex = """POINT\((.+) (.+)\)""".r

  override protected def tables(connection: Connection): Set[String] = SQLiteStore.tables(connection)

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
}

object SQLiteStore extends StoreManager {
  private lazy val spatialitePath: String = {
    val file = Files.createTempFile("mod_spatialite", ".so")
    val input = getClass.getClassLoader.getResourceAsStream("mod_spatialite.so")
    Files.copy(input, file, StandardCopyOption.REPLACE_EXISTING)
    file.toAbsolutePath.toString match {
      case s => s.substring(0, s.length - 3)
    }
  }

  def singleConnectionManager(file: Option[Path]): ConnectionManager = {
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
      val uri = s"jdbc:sqlite:$path"
      try {
        val c = config.createConnection(uri)
        c.setAutoCommit(false)

        val s = c.createStatement()
        s.executeUpdate(s"SELECT load_extension('$spatialitePath');")
        val hasGeometryColumns = this.tables(c).contains("geometry_columns")
        if (!hasGeometryColumns) s.executeUpdate("SELECT InitSpatialMetaData()")
        s.close()

        c
      } catch {
        case t: Throwable => throw new RuntimeException(s"Error establishing SQLite connection to $uri", t)
      }
    }
    SingleConnectionManager(connection)
  }

  def apply[Doc <: Document[Doc], Model <: DocumentModel[Doc]](file: Option[Path], storeMode: StoreMode): SQLiteStore[Doc, Model] = {
    new SQLiteStore[Doc, Model](
      connectionManager = singleConnectionManager(file),
      connectionShared = false,
      storeMode = storeMode
    )
  }

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         name: String,
                                                                         storeMode: StoreMode): Store[Doc, Model] = {
    db.get(SQLDatabase.Key) match {
      case Some(sqlDB) =>
        new SQLiteStore[Doc, Model](
        connectionManager = sqlDB.connectionManager,
        connectionShared = true,
        storeMode
      )
      case None => apply[Doc, Model](db.directory.map(_.resolve(s"$name.sqlite")), storeMode)
    }
  }

  private def tables(connection: Connection): Set[String] = {
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
}