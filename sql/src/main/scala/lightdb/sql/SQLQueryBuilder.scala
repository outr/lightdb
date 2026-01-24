package lightdb.sql

import fabric.rw._
import lightdb._
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.FieldAndValue
import lightdb.sql.query.{SQLPart, SQLQuery}

case class SQLQueryBuilder[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: SQLStore[Doc, Model],
                                                                              state: SQLState[Doc, Model],
                                                                              fields: List[SQLPart] = Nil,
                                                                              filters: List[SQLPart] = Nil,
                                                                              group: List[SQLPart] = Nil,
                                                                              having: List[SQLPart] = Nil,
                                                                              sort: List[SQLPart] = Nil,
                                                                              limit: Option[Int] = None,
                                                                              offset: Int,
                                                                              fromOverride: Option[List[SQLPart]] = None,
                                                                              fromSuffix: List[SQLPart] = Nil) {
  private lazy val fromParts: List[SQLPart] = fromOverride.getOrElse(List(SQLPart.Fragment(store.fqn))) ::: fromSuffix

  lazy val whereClause: List[SQLPart] = if filters.nonEmpty then {
    SQLPart.Fragment("WHERE\n") :: filters.intersperse(SQLPart.Fragment("\nAND\n"))
  } else {
    Nil
  }

  lazy val parts: List[SQLPart] = List(
    List(SQLPart.Fragment("SELECT\n\t")),
    if fields.nonEmpty then fields.intersperse(SQLPart.Fragment(", ")) else List(SQLPart.Fragment("*")),
    List(SQLPart.Fragment("\nFROM\n\t")) ::: fromParts ::: List(SQLPart.Fragment("\n")),
    whereClause,
    if group.nonEmpty then {
      SQLPart.Fragment("\nGROUP BY\n") :: group.intersperse(SQLPart.Fragment(", "))
    } else {
      Nil
    },
    if having.nonEmpty then {
      SQLPart.Fragment("\nHAVING\n") :: having.intersperse(SQLPart.Fragment("\nAND\n"))
    } else {
      Nil
    },
    if sort.nonEmpty then {
      SQLPart.Fragment("\nORDER BY\n\t") :: sort.intersperse(SQLPart.Fragment(", "))
    } else {
      Nil
    },
    limit match {
      case Some(l) => List(SQLPart.Fragment("\nLIMIT "), SQLPart.Arg(l.json))
      case None => Nil
    },
    if offset != 0 then List(SQLPart.Fragment(s"\nOFFSET $offset")) else Nil
  ).flatten

  lazy val query: SQLQuery = SQLQuery(parts)

  lazy val sql: String = query.query

  lazy val totalQuery: SQLQuery = {
    val b = copy(
      sort = Nil,
      limit = None,
      offset = 0
    )
    val pre = SQLPart.Fragment("SELECT COUNT(*) FROM (")
    val post = SQLPart.Fragment(") AS innerQuery")
    SQLQuery(pre :: b.parts ::: List(post))
  }

  def updateQuery(fields: List[FieldAndValue[Doc, _]]): SQLQuery = {
    require(fields.nonEmpty, "Update requires at least one field to update")

    val b = copy(
      sort = Nil,
      fields = List(SQLPart.Fragment("_id"))
    )
    
    val assignments: List[SQLPart] = if fields.length == 1 then {
      // Single field - no need for comma separators
      val fv = fields.head
      List(SQLPart.Fragment(s"${fv.field.name} = "), SQLPart.Arg(fv.json))
    } else {
      // Multiple fields - need comma separators between them
      fields.flatMap { fv =>
        List(SQLPart.Fragment(s"${fv.field.name} = "), SQLPart.Arg(fv.json))
      }.intersperse(SQLPart.Fragment(", "))
    }

    SQLQuery(
      parts = List(
        List(SQLPart.Fragment(s"UPDATE ${store.fqn}\n")),
        SQLPart.Fragment("SET ") :: assignments,
        List(
          SQLPart.Fragment("\nWHERE _id IN ("),
          SQLPart.Fragment("\n\tSELECT s._id"),
          SQLPart.Fragment("\n\tFROM (\n\t")
        ),
        b.parts,
        List(
          SQLPart.Fragment("\n\t) AS s"),
          SQLPart.Fragment("\n)")
        )
      ).flatten
    )
  }

  lazy val deleteQuery: SQLQuery = {
    val b = copy(
      sort = Nil,
      fields = List(SQLPart.Fragment("_id"))
    )
    SQLQuery(
      parts = List(
        SQLPart.Fragment(s"DELETE FROM ${store.fqn}\n"),
        SQLPart.Fragment("WHERE _id IN (\n"),
        SQLPart.Fragment("\tSELECT s._id\n"),
        SQLPart.Fragment("\tFROM (\n\t\t")
      ) ::: b.parts ::: List(
        SQLPart.Fragment("\n\t) AS s\n"),
        SQLPart.Fragment(")")
      )
    )
  }
}