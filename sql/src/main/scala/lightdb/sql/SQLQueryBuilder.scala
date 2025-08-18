package lightdb.sql

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.FieldAndValue
import lightdb.sql.query.SQLQuery

import java.sql.SQLException

case class SQLQueryBuilder[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: SQLStore[Doc, Model],
                                                                              state: SQLState[Doc, Model],
                                                                              fields: List[SQLPart] = Nil,
                                                                              filters: List[SQLPart] = Nil,
                                                                              group: List[SQLPart] = Nil,
                                                                              having: List[SQLPart] = Nil,
                                                                              sort: List[SQLPart] = Nil,
                                                                              limit: Option[Int] = None,
                                                                              offset: Int) {
  lazy val whereClauses: List[SQLPart] = filters.filter(p => p.sql.nonEmpty || p.args.nonEmpty)

  lazy val sql: String = {
    val b = new StringBuilder
    b.append("SELECT\n")
    b.append(s"\t${fields.map(_.sql).mkString(", ")}\n")
    b.append("FROM\n")
    b.append(s"\t${store.fqn}\n")
    whereClauses.zipWithIndex.foreach {
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

  lazy val totalQuery: SQLQuery = {
    val b = copy(
      sort = Nil,
      limit = None,
      offset = 0
    )
    val pre = "SELECT COUNT(*) FROM ("
    val post = ") AS innerQuery"
    SQLQuery.parse(s"$pre${b.sql}$post").fillPlaceholder(b.args.map(_.json): _*)
  }

  def updateQuery(fields: List[FieldAndValue[Doc, _]]): SQLQuery = {
    require(fields.nonEmpty, "Update requires at least one field to update")

    val b = copy(
      sort = Nil,
      fields = List(SQLPart("_id"))
    )
    val assignments = fields.map { fv => s"${fv.field.name} = ?" }.mkString(", ")
    val query =
      s"""UPDATE ${store.fqn}
         |SET $assignments
         |WHERE _id IN (
         |  SELECT s._id
         |  FROM (
         |    ${b.sql}
         |  ) AS s
         |)
         |""".stripMargin
    val args = fields.map(fv => fv.json) ::: b.args.map(_.json)
    SQLQuery.parse(query).fillPlaceholder(args: _*)
  }

  lazy val deleteQuery: SQLQuery = {
    val b = copy(
      sort = Nil,
      fields = List(SQLPart("_id"))
    )
    val query =
      s"""DELETE FROM ${store.fqn}
         |WHERE _id IN (
         |  SELECT s._id
         |  FROM (
         |    ${b.sql}
         |  ) AS s
         |)
         |""".stripMargin
    SQLQuery.parse(query).fillPlaceholder(b.args.map(_.json): _*)
  }

  def toQuery: SQLQuery = SQLQuery.parse(sql).fillPlaceholder(args.map(_.json): _*)
}