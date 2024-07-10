package lightdb.sql

import lightdb.collection.Collection
import lightdb.doc.Document

import java.sql.{Connection, ResultSet}

case class SQLQueryBuilder[Doc <: Document[Doc]](collection: Collection[Doc, _],
                                transaction: SQLTransaction[Doc],
                                fields: List[SQLPart] = Nil,
                                filters: List[SQLPart] = Nil,
                                group: List[SQLPart] = Nil,
                                having: List[SQLPart] = Nil,
                                sort: List[SQLPart] = Nil,
                                limit: Option[Int] = None,
                                offset: Int) {
  lazy val sql: String = {
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
    b.toString()
  }

  lazy val args: List[SQLArg] = (fields ::: filters ::: group ::: having ::: sort).flatMap(_.args)

  def queryTotal(connection: Connection): Int = {
    val b = copy(
      sort = Nil,
      limit = None,
      offset = 0
    )
    val rs = b.executeInternal(connection, "SELECT COUNT(*) FROM (", ") AS innerQuery")
    try {
      rs.next()
      rs.getInt(1)
    } finally {
      rs.close()
    }
  }

  def execute(connection: Connection): ResultSet = executeInternal(connection)

  private def executeInternal(connection: Connection, pre: String = "", post: String = ""): ResultSet = {
    scribe.debug(s"Executing Query: $sql (${args.mkString(", ")})")
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
      try {
        val ps = connection.prepareStatement(s"$pre$sql$post")
        transaction.register(ps)
        ps
      } catch {
        case t: Throwable => throw new RuntimeException(s"Failed to execute query:\n$sql\nParams: ${args.mkString(", ")}", t)
      }
    }
    args.zipWithIndex.foreach {
      case (value, index) => value.set(ps, index + 1)
    }
    ps.executeQuery()
  }
}
