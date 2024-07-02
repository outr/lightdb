package next

import fabric._

import java.nio.file.Path
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.language.implicitConversions

case class SQLiteStore[Doc, Model <: DocModel[Doc]](file: Path, converter: Converter[ResultSet, Doc]) extends Store[Doc, Model] {
  private var collection: Collection[Doc, Model] = _

  private lazy val connection: Connection = {
    val path = file.toFile.getAbsolutePath
    val c = DriverManager.getConnection(s"jdbc:sqlite:$path")
    c.setAutoCommit(false)
    c
  }

  override def init(collection: Collection[Doc, Model]): Unit = {
    this.collection = collection
    executeUpdate(s"CREATE TABLE IF NOT EXISTS ${collection.name}(${collection.model.fields.map(_.name).mkString(", ")})")
    collection.model.fields.foreach {
      case _: Field.Basic[Doc, _] => // Nothing to do
      case index: Field.Index[Doc, _] =>
        executeUpdate(s"CREATE INDEX IF NOT EXISTS ${index.name}_idx ON ${collection.name}(${index.name})")
      case index: Field.Unique[Doc, _] =>
        executeUpdate(s"CREATE UNIQUE INDEX IF NOT EXISTS ${index.name}_idx ON ${collection.name}(${index.name})")
    }
  }

  private implicit def transaction2Impl(transaction: Transaction[Doc]): SQLiteTransaction[Doc] = transaction.asInstanceOf[SQLiteTransaction[Doc]]

  override def createTransaction(): Transaction[Doc] = new SQLiteTransaction[Doc]

  override def releaseTransaction(transaction: Transaction[Doc]): Unit = transaction.close()

  private lazy val insertSQL: String =
    s"INSERT OR REPLACE INTO ${collection.name}(${collection.model.fields.map(_.name).mkString(", ")}) VALUES(${collection.model.fields.map(_ => "?").mkString(", ")})"

  private def preparedStatement(implicit transaction: Transaction[Doc]): PreparedStatement = transaction.synchronized {
    if (transaction.ps == null) {
      transaction.ps = connection.prepareStatement(insertSQL)
    }
    transaction.ps
  }

  override def set(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    val ps = preparedStatement
    collection.model.fields.zipWithIndex.foreach {
      case (field, index) =>
        field.get(doc) match {
          case s: String => ps.setString(index + 1, s)
          case i: Int => ps.setInt(index + 1, i)
          case v => throw new RuntimeException(s"Unsupported type: $v (${v.getClass.getName})")
        }
    }
    ps.addBatch()
    transaction.batch += 1
    if (transaction.batch >= collection.maxInsertBatch) {
      ps.executeBatch()
      transaction.batch = 0
    }
  }

  override def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = {
    val s = connection.createStatement()
    transaction.register(s)
    val rs = s.executeQuery(s"SELECT * FROM ${collection.name}")
    transaction.register(rs)
    rs2Iterator(rs, Conversion.Doc)
  }

  private def rs2Iterator[V](rs: ResultSet, conversion: Conversion[V]): Iterator[V] = new Iterator[V] {
    override def hasNext: Boolean = rs.next()

    override def next(): V = conversion match {
      case Conversion.Value(field) => rs.getObject(field.name).asInstanceOf[V]
      case Conversion.Doc => converter.convert(rs).asInstanceOf[V]
      case Conversion.Converted(c) => c(converter.convert(rs))
      case Conversion.Json(fields) =>
        obj(fields.map(f => f.name -> toJson(rs.getObject(f.name))): _*).asInstanceOf[V]
    }
  }

  private def toJson(value: Any): Json = value match {
    case null => Null
    case s: String => str(s)
    case i: Int => num(i)
    case l: Long => num(l)
    case d: Double => num(d)
    case b: Boolean => bool(b)
  }

  override def doSearch[V](query: Query[Doc, Model], conversion: Conversion[V])
                          (implicit transaction: Transaction[Doc]): SearchResults[Doc, V] = {
    val fields = conversion match {
      case Conversion.Value(field) => List(field)
      case Conversion.Doc | Conversion.Converted(_) => collection.model.fields
      case Conversion.Json(fields) => fields
    }
    val b = SQLQueryBuilder(
      collection = collection,
      transaction = transaction,
      fields = fields.map(f => SQLPart(f.name)),
      filters = query.filter.map(filter2Part).toList,
      group = Nil,
      having = Nil,
      sort = query.sort.collect {
        case Sort.ByIndex(index, direction) =>
          val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
          SQLPart(s"${index.name} $dir", Nil)
      },
      limit = query.limit,
      offset = query.offset
    )
    val rs = b.execute(connection)
    transaction.register(rs)
    val total = if (query.countTotal) {
      Some(b.queryTotal(connection))
    } else {
      None
    }
    val iterator = rs2Iterator(rs, conversion)
    SearchResults(
      offset = query.offset,
      limit = query.limit,
      total = total,
      iterator = iterator,
      transaction = transaction
    )
  }

  private def filter2Part(f: Filter[Doc]): SQLPart = f match {
    case f: Filter.Equals[Doc, _] => SQLPart(s"${f.field.name} = ?", List(f.value))
    case f: Filter.In[Doc, _] => SQLPart(s"${f.field.name} IN (${f.values.map(_ => "?").mkString(", ")})", f.values.toList)
    case f: Filter.Combined[Doc] =>
      val parts = f.filters.map(f => filter2Part(f))
      SQLPart(parts.map(_.sql).mkString(" AND "), parts.flatMap(_.args))
    case f: Filter.RangeLong[Doc] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.field.name} BETWEEN ? AND ?", List(from, to))
      case (None, Some(to)) => SQLPart(s"${f.field.name} <= ?", List(to))
      case (Some(from), None) => SQLPart(s"${f.field.name} >= ?", List(from))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: Filter.RangeDouble[Doc] => (f.from, f.to) match {
      case (Some(from), Some(to)) => SQLPart(s"${f.field.name} BETWEEN ? AND ?", List(from, to))
      case (None, Some(to)) => SQLPart(s"${f.field.name} <= ?", List(to))
      case (Some(from), None) => SQLPart(s"${f.field.name} >= ?", List(from))
      case _ => throw new UnsupportedOperationException(s"Invalid: $f")
    }
    case f: Filter.Parsed[Doc, _] => throw new UnsupportedOperationException("Parsed not supported in SQL!")
    case f: Filter.Distance[Doc] => throw new UnsupportedOperationException("Distance not supported in SQL!")
  }

  private def executeUpdate(sql: String): Unit = {
    val s = connection.createStatement()
    try {
      s.executeUpdate(sql)
    } finally {
      s.close()
    }
  }

  override def dispose(): Unit = {
    connection.commit()
    connection.close()
  }
}
