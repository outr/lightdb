package lightdb.sql

import fabric._
import fabric.rw.Asable
import lightdb.collection.Collection
import lightdb.doc.{DocModel, JsonConversion}
import lightdb.sql.connect.{ConnectionManager, SQLConfig}
import lightdb.store.Store
import lightdb.{Field, Filter, Query, SearchResults, Sort, SortDirection, Transaction}

import java.sql.{PreparedStatement, ResultSet}
import scala.language.implicitConversions

trait SQLStore[Doc, Model <: DocModel[Doc]] extends Store[Doc, Model] {
  private var collection: Collection[Doc, Model] = _

  protected def config: SQLConfig
  protected def connectionManager: ConnectionManager[Doc]

  override def init(collection: Collection[Doc, Model]): Unit = {
    this.collection = collection
    collection.transaction { implicit transaction =>
      executeUpdate(s"CREATE TABLE IF NOT EXISTS ${collection.name}(${collection.model.fields.map(_.name).mkString(", ")})")
      collection.model.fields.foreach {
        case _: Field.Basic[Doc, _] => // Nothing to do
        case index: Field.Index[Doc, _] =>
          executeUpdate(s"CREATE INDEX IF NOT EXISTS ${index.name}_idx ON ${collection.name}(${index.name})")
        case index: Field.Unique[Doc, _] =>
          executeUpdate(s"CREATE UNIQUE INDEX IF NOT EXISTS ${index.name}_idx ON ${collection.name}(${index.name})")
      }
    }
  }

  override def createTransaction(): Transaction[Doc] = SQLTransaction[Doc](connectionManager)

  override def releaseTransaction(transaction: Transaction[Doc]): Unit = transaction.close()

  private lazy val insertSQL: String =
    s"INSERT OR REPLACE INTO ${collection.name}(${collection.model.fields.map(_.name).mkString(", ")}) VALUES(${collection.model.fields.map(_ => "?").mkString(", ")})"

  private def preparedStatement(implicit transaction: Transaction[Doc]): PreparedStatement = transaction.synchronized {
    if (transaction.ps == null) {
      val connection = connectionManager.getConnection
      transaction.ps = connection.prepareStatement(insertSQL)
    }
    transaction.ps
  }

  override def set(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    val ps = preparedStatement
    collection.model.fields.zipWithIndex.foreach {
      case (field, index) =>
        val value = field.get(doc)
        SQLQueryBuilder.setValue(ps, index, value)
    }
    ps.addBatch()
    transaction.batch += 1
    if (transaction.batch >= collection.maxInsertBatch) {
      ps.executeBatch()
      transaction.batch = 0
    }
  }

  override def get[V](field: Field.Unique[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Option[Doc] = {
    val b = new SQLQueryBuilder[Doc](
      collection = collection,
      transaction = transaction,
      fields = collection.model.fields.map(f => SQLPart(f.name)),
      filters = List(filter2Part(field === value)),
      group = Nil,
      having = Nil,
      sort = Nil,
      limit = Some(1),
      offset = 0
    )
    val rs = b.execute(connectionManager.getConnection)
    try {
      if (rs.next()) {
        Some(getDoc(rs))
      } else {
        None
      }
    } finally {
      rs.close()
    }
  }

  override def delete[V](field: Field.Unique[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Boolean = {
    val connection = connectionManager.getConnection
    val ps = connection.prepareStatement(s"DELETE FROM ${collection.name} WHERE ${field.name} = ?")
    try {
      SQLQueryBuilder.setValue(ps, 0, value)
      ps.executeUpdate() > 0
    } finally {
      ps.close()
    }
  }

  override def count(implicit transaction: Transaction[Doc]): Int = {
    val rs = executeQuery(s"SELECT COUNT(*) FROM ${collection.name}")
    try {
      rs.next()
      rs.getInt(1)
    } finally {
      rs.close()
    }
  }

  override def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = {
    val connection = connectionManager.getConnection
    val s = connection.createStatement()
    transaction.register(s)
    val rs = s.executeQuery(s"SELECT * FROM ${collection.name}")
    transaction.register(rs)
    rs2Iterator(rs, Conversion.Doc)
  }

  private def getDoc(rs: ResultSet): Doc = collection.model match {
    case c: JsonConversion[Doc] =>
      val values = collection.model.fields.map { field =>
        field.name -> toJson(rs.getObject(field.name))
      }
      c.convertFromJson(obj(values: _*))
    case c: SQLConversion[Doc] => c.convertFromSQL(rs)
    case _ =>
      val map = collection.model.fields.map { field =>
        field.name -> obj2Value(rs.getObject(field.name))
      }.toMap
      collection.model.map2Doc(map)
  }

  private def rs2Iterator[V](rs: ResultSet, conversion: Conversion[V]): Iterator[V] = new Iterator[V] {
    override def hasNext: Boolean = rs.next()

    override def next(): V = conversion match {
      case Conversion.Value(field) => toJson(rs.getObject(field.name)).as[V](field.rw)
      case Conversion.Doc => getDoc(rs).asInstanceOf[V]
      case Conversion.Converted(c) => c(getDoc(rs))
      case Conversion.Json(fields) =>
        obj(fields.map(f => f.name -> toJson(rs.getObject(f.name))): _*).asInstanceOf[V]
    }
  }

  private def obj2Value(obj: Any): Any = obj match {
    case s: String => s
    case i: java.lang.Integer => i.intValue()
    case _ => throw new RuntimeException(s"Unsupported object: $obj (${obj.getClass.getName})")
  }

  private def toJson(value: Any): Json = obj2Value(value) match {
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
        case Sort.ByField(index, direction) =>
          val dir = if (direction == SortDirection.Descending) "DESC" else "ASC"
          SQLPart(s"${index.name} $dir", Nil)
      },
      limit = query.limit,
      offset = query.offset
    )
    val connection = connectionManager.getConnection
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

  private def executeUpdate(sql: String)(implicit transaction: Transaction[Doc]): Unit = {
    val connection = connectionManager.getConnection
    val s = connection.createStatement()
    try {
      s.executeUpdate(sql)
    } finally {
      s.close()
    }
  }

  private def executeQuery(sql: String)(implicit transaction: Transaction[Doc]): ResultSet = {
    val connection = connectionManager.getConnection
    val s = connection.createStatement()
    transaction.register(s)
    s.executeQuery(sql)
  }

  override def truncate()(implicit transaction: Transaction[Doc]): Int = {
    val connection = connectionManager.getConnection
    val ps = connection.prepareStatement(s"DELETE FROM ${collection.name}")
    try {
      ps.executeUpdate()
    } finally {
      ps.close()
    }
  }

  override def dispose(): Unit = {
  }
}
