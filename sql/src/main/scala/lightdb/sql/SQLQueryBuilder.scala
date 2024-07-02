package lightdb.sql

import lightdb.collection.Collection
import lightdb.sql.SQLQueryBuilder.setValue

import java.sql.{Connection, PreparedStatement, ResultSet}

case class SQLQueryBuilder[Doc](collection: Collection[Doc, _],
                                transaction: SQLTransaction[Doc],
                                fields: List[SQLPart] = Nil,
                                filters: List[SQLPart] = Nil,
                                group: List[SQLPart] = Nil,
                                having: List[SQLPart] = Nil,
                                sort: List[SQLPart] = Nil,
                                limit: Option[Int] = None,
                                offset: Int) {
  def queryTotal(connection: Connection): Int = {
    val b = copy(
      fields = List(SQLPart("COUNT(*) AS count", Nil)),
      group = Nil,
      having = Nil,
      sort = Nil,
      limit = None,
      offset = 0
    )
    val rs = b.execute(connection)
    try {
      rs.next()
      rs.getInt(1)
    } finally {
      rs.close()
    }
  }

  def execute(connection: Connection): ResultSet = {
    val b = new StringBuilder
    b.append("SELECT\n")
    b.append(s"\t${fields.map(_.sql).mkString(", ")}\n")
    b.append("FROM\n")
    b.append(s"\t${collection.name}\n")
    filters.zipWithIndex.foreach {
      case (f, index) =>
        if (index == 0) {
          b.append("WHERE\n")
        } else {
          b.append("AND\n")
        }
        b.append(s"\t${f.sql}\n")
    }
    if (group.nonEmpty) {
      b.append("GROUP BY\n\t")
      b.append(group.map(_.sql).mkString(", "))
      b.append('\n')
    }
    having.zipWithIndex.foreach {
      case (f, index) =>
        if (index == 0) {
          b.append("HAVING\n")
        } else {
          b.append("AND\n")
        }
        b.append(s"\t${f.sql}\n")
    }
    if (sort.nonEmpty) {
      b.append("ORDER BY\n\t")
      b.append(sort.map(_.sql).mkString(", "))
      b.append('\n')
    }
    limit.foreach { l =>
      b.append("LIMIT\n")
      b.append(s"\t$l\n")
    }
    if (offset > 0) {
      b.append("OFFSET\n")
      b.append(s"\t$offset\n")
    }
    val args = (fields ::: filters ::: group ::: having ::: sort).flatMap(_.args)
    val sql = b.toString()
    val ps = if (collection.cacheQueries) {
      transaction.synchronized {
        transaction.cache.get(sql) match {
          case Some(ps) => ps
          case None =>
            val ps = connection.prepareStatement(sql)
            transaction.register(ps)
            transaction.cache += sql -> ps
            ps
        }
      }
    } else {
      val ps = connection.prepareStatement(sql)
      transaction.register(ps)
      ps
    }
    args.zipWithIndex.foreach {
      case (value, index) => setValue(ps, index, value)
    }
    ps.executeQuery()
  }
}

object SQLQueryBuilder {
  def setValue(ps: PreparedStatement, index: Int, value: Any): Unit = value match {
    case s: String => ps.setString(index + 1, s)
    case i: Int => ps.setInt(index + 1, i)
    case _ => throw new UnsupportedOperationException(s"Unsupported value: $value (${value.getClass.getName})")
  }
}
