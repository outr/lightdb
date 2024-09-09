package lightdb.sql

import fabric._
import fabric.define.DefType
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._
import lightdb.collection.Collection
import lightdb.distance.Distance
import lightdb.sql.connect.{ConnectionManager, DBCPConnectionManager, SQLConfig, SingleConnectionManager}
import lightdb.{Field, LightDB, SortDirection}
import lightdb.doc.{Document, DocumentModel}
import lightdb.filter.Filter
import lightdb.spatial.{Geo, Spatial}
import lightdb.store.{Conversion, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import org.sqlite.{Collation, SQLiteConfig, SQLiteOpenMode}
import org.sqlite.SQLiteConfig.{JournalMode, LockingMode, SynchronousMode, TransactionMode}

import java.io.File
import java.nio.file.{Files, Path, StandardCopyOption}
import java.sql.Connection
import java.util.regex.Pattern

class SQLiteStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](val connectionManager: ConnectionManager,
                                                                     val connectionShared: Boolean,
                                                                     val storeMode: StoreMode) extends SQLStore[Doc, Model] {
  override protected def initTransaction()(implicit transaction: Transaction[Doc]): Unit = {
    val c = connectionManager.getConnection
    if (hasSpatial) {
      scribe.info(s"${collection.name} has spatial features. Enabling...")
      org.sqlite.Function.create(c, "DISTANCE", new org.sqlite.Function() {
        override def xFunc(): Unit = {
          def s(index: Int): List[Geo] = Option(value_text(index))
            .map(s => JsonParser(s))
            .map {
              case Arr(vector, _) => vector.toList.map(_.as[Geo])
              case json => List(json.as[Geo])
            }
            .getOrElse(Nil)
          val shapes1 = s(0)
          val shapes2 = s(1)
          val distances = shapes1.flatMap { geo1 =>
            shapes2.map { geo2 =>
              Spatial.distance(geo1, geo2)
            }
          }
          result(JsonFormatter.Compact(distances.json))
        }
      })
      org.sqlite.Function.create(c, "DISTANCE_LESS_THAN", new org.sqlite.Function() {
        override def xFunc(): Unit = {
          val distances = Option(value_text(0))
            .map(s => JsonParser(s).as[List[Distance]])
            .getOrElse(Nil)
          val value = value_text(1).toDouble
          val b = distances.exists(d => d.valueInMeters <= value)
          result(if (b) 1 else 0)
        }
      })
      org.sqlite.Collation.create(c, "DISTANCE_SORT_ASCENDING", new Collation() {
        override def xCompare(str1: String, str2: String): Int = {
          val min1 = JsonParser(str1).as[List[Double]].min
          val min2 = JsonParser(str2).as[List[Double]].min
          min1.compareTo(min2)
        }
      })
      org.sqlite.Collation.create(c, "DISTANCE_SORT_DESCENDING", new Collation() {
        override def xCompare(str1: String, str2: String): Int = {
          val min1 = JsonParser(str1).as[List[Double]].min
          val min2 = JsonParser(str2).as[List[Double]].min
          min2.compareTo(min1)
        }
      })
    }
    org.sqlite.Function.create(c, "REGEXP", new org.sqlite.Function() {
      override def xFunc(): Unit = {
        val expression = value_text(0)
        val value = Option(value_text(1)).getOrElse("")
        val pattern = Pattern.compile(expression)
        result(if (pattern.matcher(value).find()) 1 else 0)
      }
    })
    super.initTransaction()
  }

  override protected def tables(connection: Connection): Set[String] = SQLiteStore.tables(connection)

  override protected def extraFieldsForDistance(d: Conversion.Distance[Doc, _]): List[SQLPart] =
    List(SQLPart(s"DISTANCE(${d.field.name}, ?) AS ${d.field.name}Distance", List(SQLArg.JsonArg(d.from.json))))

  override protected def distanceFilter(f: Filter.Distance[Doc]): SQLPart =
    SQLPart(s"DISTANCE_LESS_THAN(${f.fieldName}Distance, ?)", List(SQLArg.DoubleArg(f.radius.valueInMeters)))

  override protected def sortByDistance[G <: Geo](field: Field[_, List[G]], direction: SortDirection): SQLPart = direction match {
    case SortDirection.Ascending => SQLPart(s"${field.name}Distance COLLATE DISTANCE_SORT_ASCENDING")
    case SortDirection.Descending => SQLPart(s"${field.name}Distance COLLATE DISTANCE_SORT_DESCENDING")
  }
}

object SQLiteStore extends StoreManager {
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