package lightdb.sql

import lightdb.doc.{Document, DocumentModel}

import java.sql.SQLException

case class SQLQueryBuilder[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: SQLStore[Doc, Model],
                                                                              state: SQLState[Doc, Model],
                                                                              fields: List[SQLPart] = Nil,
                                                                              filters: List[SQLPart] = Nil,
                                                                              group: List[SQLPart] = Nil,
                                                                              having: List[SQLPart] = Nil,
                                                                              sort: List[SQLPart] = Nil,
                                                                              limit: Option[Int] = None,
                                                                              offset: Int) extends SQL {
  override lazy val sql: String = {
    val b = new StringBuilder
    b.append("SELECT\n")
    b.append(s"\t${fields.map(_.sql).mkString(", ")}\n")
    b.append("FROM\n")
    b.append(s"\t${store.name}\n")
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

  override lazy val args: List[SQLArg] = (fields ::: filters ::: group ::: having ::: sort).flatMap(_.args)

  lazy val totalQuery: SQL = {
    val b = copy(
      sort = Nil,
      limit = None,
      offset = 0
    )
    val pre = "SELECT COUNT(*) FROM ("
    val post = ") AS innerQuery"
    SQLPart(
      sql = s"$pre${b.sql}$post",
      args = b.args
    )
  }
}