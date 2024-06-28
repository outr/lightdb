package lightdb.sql

import lightdb.collection.Collection
import lightdb.document.Document

import java.sql.{Connection, ResultSet}

case class SQLQueryBuilder[D <: Document[D]](collection: Collection[D, _],
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
//    scribe.info(s"${b.toString()} (${args.mkString(", ")})")
    val ps = connection.prepareStatement(b.toString())
    args.zipWithIndex.foreach {
      case (value, index) => SQLIndexer.setValue(ps, index + 1, value)
    }
    ps.executeQuery()
  }
}
