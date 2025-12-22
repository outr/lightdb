package lightdb.sql.dsl

import lightdb.ListExtras
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.sql.SQLStore
import lightdb.store.{Collection, Store}
import lightdb.sql.query.{SQLPart, SQLQuery}

/**
 * A minimal, structural SQL DSL that builds parameterized queries (via [[SQLPart.Arg]])
 * and renders to [[lightdb.sql.query.SQLQuery]].
 *
 * This is intended for ad-hoc SQL where building giant SQL strings is undesirable.
 * It is complementary to [[lightdb.sql.SQLQueryBuilder]], which is store/query-aware.
 */
object SQLDsl {
  // ----------------------------
  // Identifiers
  // ----------------------------
  sealed trait Ident {
    def value: String
  }

  object Ident {
    final case class Table(name: String, alias: Option[String] = None) extends Ident {
      override def value: String = alias match {
        case Some(a) => s"$name $a"
        case None => name
      }
      def as(a: String): Table = copy(alias = Some(a))
      def ref: String = alias.getOrElse(name)
    }

    final case class Column(name: String, qualifier: Option[String] = None) extends Ident {
      override def value: String = qualifier match {
        case Some(q) => s"$q.$name"
        case None => name
      }
    }

    def table(name: String): Table = Table(name)
    def col(name: String): Column = Column(name)
    def col(qualifier: String, name: String): Column = Column(name = name, qualifier = Some(qualifier))
  }

  // ----------------------------
  // Expressions (WHERE / HAVING)
  // ----------------------------
  sealed trait Expr {
    def and(other: Expr): Expr = Expr.And(this, other)
    def or(other: Expr): Expr = Expr.Or(this, other)
    def not: Expr = Expr.Not(this)

    private[dsl] def render: SQLQuery
  }

  object Expr {
    final case class Raw(sql: String) extends Expr {
      override private[dsl] def render: SQLQuery = SQLQuery(List(SQLPart.Fragment(sql)))
    }

    sealed trait Term {
      def renderAsPart: SQLPart
    }

    object Term {
      final case class V(value: Value) extends Term {
        override def renderAsPart: SQLPart = value.render
      }
      final case class C(column: Ident.Column) extends Term {
        override def renderAsPart: SQLPart = SQLPart.Fragment(column.value)
      }
      def value(v: Value): Term = V(v)
      def col(c: Ident.Column): Term = C(c)
    }

    final case class Eq(left: Ident.Column, right: Term) extends Expr {
      override private[dsl] def render: SQLQuery = SQLQuery(List(
        SQLPart.Fragment(left.value),
        SQLPart.Fragment(" = "),
        right.renderAsPart
      ))
    }

    final case class Neq(left: Ident.Column, right: Term) extends Expr {
      override private[dsl] def render: SQLQuery = SQLQuery(List(
        SQLPart.Fragment(left.value),
        SQLPart.Fragment(" <> "),
        right.renderAsPart
      ))
    }

    final case class Lt(left: Ident.Column, right: Term) extends Expr {
      override private[dsl] def render: SQLQuery = SQLQuery(List(
        SQLPart.Fragment(left.value),
        SQLPart.Fragment(" < "),
        right.renderAsPart
      ))
    }

    final case class Lte(left: Ident.Column, right: Term) extends Expr {
      override private[dsl] def render: SQLQuery = SQLQuery(List(
        SQLPart.Fragment(left.value),
        SQLPart.Fragment(" <= "),
        right.renderAsPart
      ))
    }

    final case class Gt(left: Ident.Column, right: Term) extends Expr {
      override private[dsl] def render: SQLQuery = SQLQuery(List(
        SQLPart.Fragment(left.value),
        SQLPart.Fragment(" > "),
        right.renderAsPart
      ))
    }

    final case class Gte(left: Ident.Column, right: Term) extends Expr {
      override private[dsl] def render: SQLQuery = SQLQuery(List(
        SQLPart.Fragment(left.value),
        SQLPart.Fragment(" >= "),
        right.renderAsPart
      ))
    }

    final case class Like(left: Ident.Column, pattern: Value) extends Expr {
      override private[dsl] def render: SQLQuery = SQLQuery(List(
        SQLPart.Fragment(left.value),
        SQLPart.Fragment(" LIKE "),
        pattern.render
      ))
    }

    final case class IsNull(col: Ident.Column) extends Expr {
      override private[dsl] def render: SQLQuery = SQLQuery(List(
        SQLPart.Fragment(col.value),
        SQLPart.Fragment(" IS NULL")
      ))
    }

    final case class IsNotNull(col: Ident.Column) extends Expr {
      override private[dsl] def render: SQLQuery = SQLQuery(List(
        SQLPart.Fragment(col.value),
        SQLPart.Fragment(" IS NOT NULL")
      ))
    }

    final case class In(col: Ident.Column, values: List[Value]) extends Expr {
      override private[dsl] def render: SQLQuery = {
        val rendered = values.map(_.render)
        val inner: List[SQLPart] =
          if (rendered.isEmpty) List(SQLPart.Fragment("NULL"))
          else rendered.intersperse(SQLPart.Fragment(", "))

        SQLQuery(
          List(
            SQLPart.Fragment(col.value),
            SQLPart.Fragment(" IN (")
          ) ::: inner ::: List(SQLPart.Fragment(")"))
        )
      }
    }

    final case class And(left: Expr, right: Expr) extends Expr {
      override private[dsl] def render: SQLQuery = SQLQuery(List(
        SQLPart.Fragment("("),
        left.render,
        SQLPart.Fragment(" AND "),
        right.render,
        SQLPart.Fragment(")")
      ))
    }

    final case class Or(left: Expr, right: Expr) extends Expr {
      override private[dsl] def render: SQLQuery = SQLQuery(List(
        SQLPart.Fragment("("),
        left.render,
        SQLPart.Fragment(" OR "),
        right.render,
        SQLPart.Fragment(")")
      ))
    }

    final case class Not(expr: Expr) extends Expr {
      override private[dsl] def render: SQLQuery = SQLQuery(List(
        SQLPart.Fragment("NOT("),
        expr.render,
        SQLPart.Fragment(")")
      ))
    }
  }

  // ----------------------------
  // Values
  // ----------------------------
  sealed trait Value {
    private[dsl] def render: SQLPart
  }

  object Value {
    final case class Arg(value: Any) extends Value {
      override private[dsl] def render: SQLPart = SQLPart.Arg(SQLQuery.toJson(value))
    }

    final case class Raw(sql: String) extends Value {
      override private[dsl] def render: SQLPart = SQLPart.Fragment(sql)
    }
  }

  // ----------------------------
  // SELECT query
  // ----------------------------
  final case class OrderBy(col: Ident.Column, direction: SortDirection)

  sealed trait SortDirection { def sql: String }
  object SortDirection {
    case object Asc extends SortDirection { override val sql: String = "ASC" }
    case object Desc extends SortDirection { override val sql: String = "DESC" }
  }

  sealed trait JoinType { def sql: String }
  object JoinType {
    case object Inner extends JoinType { override val sql: String = "JOIN" }
    case object Left extends JoinType { override val sql: String = "LEFT JOIN" }
    case object Right extends JoinType { override val sql: String = "RIGHT JOIN" }
    case object Full extends JoinType { override val sql: String = "FULL JOIN" }
    case object Cross extends JoinType { override val sql: String = "CROSS JOIN" }
  }

  final case class Join(joinType: JoinType, table: Ident.Table, on: Option[Expr])

  final case class Select(
    columns: List[Ident.Column] = Nil,
    from: Option[Ident.Table] = None,
    joins: List[Join] = Nil,
    where: Option[Expr] = None,
    orderBy: List[OrderBy] = Nil,
    limit: Option[Int] = None,
    offset: Option[Int] = None
  ) {
    def select(cols: Ident.Column*): Select = copy(columns = cols.toList)
    def from(table: Ident.Table): Select = copy(from = Some(table))

    def join(table: Ident.Table, on: Expr): Select =
      copy(joins = joins :+ Join(JoinType.Inner, table, Some(on)))
    def leftJoin(table: Ident.Table, on: Expr): Select =
      copy(joins = joins :+ Join(JoinType.Left, table, Some(on)))
    def rightJoin(table: Ident.Table, on: Expr): Select =
      copy(joins = joins :+ Join(JoinType.Right, table, Some(on)))
    def fullJoin(table: Ident.Table, on: Expr): Select =
      copy(joins = joins :+ Join(JoinType.Full, table, Some(on)))
    def crossJoin(table: Ident.Table): Select =
      copy(joins = joins :+ Join(JoinType.Cross, table, None))

    def where(expr: Expr): Select = copy(where = Some(expr))
    def andWhere(expr: Expr): Select = copy(where = Some(where.map(_.and(expr)).getOrElse(expr)))
    def orWhere(expr: Expr): Select = copy(where = Some(where.map(_.or(expr)).getOrElse(expr)))
    def orderBy(cols: OrderBy*): Select = copy(orderBy = cols.toList)
    def limit(n: Int): Select = copy(limit = Some(n))
    def offset(n: Int): Select = copy(offset = Some(n))

    def toSQLQuery: SQLQuery = {
      val cols: List[SQLPart] =
        if (columns.isEmpty) List(SQLPart.Fragment("*"))
        else columns.map(c => SQLPart.Fragment(c.value)).intersperse(SQLPart.Fragment(", "))

      val fromPart: List[SQLPart] = from match {
        case Some(t) => List(SQLPart.Fragment(" FROM "), SQLPart.Fragment(t.value))
        case None => throw new RuntimeException("SELECT requires FROM(...)")
      }

      val joinPart: List[SQLPart] = joins.flatMap { j =>
        val base: List[SQLPart] = List(
          SQLPart.Fragment(" "),
          SQLPart.Fragment(j.joinType.sql),
          SQLPart.Fragment(" "),
          SQLPart.Fragment(j.table.value)
        )
        j.on match {
          case Some(onExpr) =>
            base ::: List(SQLPart.Fragment(" ON "), onExpr.render)
          case None =>
            base
        }
      }

      val wherePart: List[SQLPart] = where match {
        case Some(w) => List(SQLPart.Fragment(" WHERE "), w.render)
        case None => Nil
      }

      val orderPart: List[SQLPart] =
        if (orderBy.isEmpty) Nil
        else {
          val parts: List[SQLPart] = orderBy.map { ob =>
            SQLQuery(List(
              SQLPart.Fragment(ob.col.value),
              SQLPart.Fragment(" "),
              SQLPart.Fragment(ob.direction.sql)
            ))
          }
          SQLPart.Fragment(" ORDER BY ") :: parts.intersperse(SQLPart.Fragment(", "))
        }

      val limitPart: List[SQLPart] = limit match {
        case Some(l) => List(SQLPart.Fragment(" LIMIT "), SQLPart.Arg(SQLQuery.toJson(l)))
        case None => Nil
      }

      val offsetPart: List[SQLPart] = offset match {
        case Some(o) => List(SQLPart.Fragment(" OFFSET "), SQLPart.Arg(SQLQuery.toJson(o)))
        case None => Nil
      }

      SQLQuery(
        List(SQLPart.Fragment("SELECT ")) ::: cols ::: fromPart ::: joinPart ::: wherePart ::: orderPart ::: limitPart ::: offsetPart
      )
    }
  }

  // ----------------------------
  // Convenience constructors
  // ----------------------------
  def select(cols: Ident.Column*): Select = Select(columns = cols.toList)

  def table(name: String): Ident.Table = Ident.table(name)
  def table(store: Store[?, ?]): Ident.Table = store match {
    case sql: SQLStore[?, ?] => Ident.table(sql.fqn)
    case other => Ident.table(other.name)
  }
  def table(collection: Collection[?, ?]): Ident.Table = table(collection.asInstanceOf[Store[?, ?]])

  def col(name: String): Ident.Column = Ident.col(name)
  def col(qualifier: String, name: String): Ident.Column = Ident.col(qualifier, name)
  def col[Doc <: Document[Doc], V](field: Field[Doc, V]): Ident.Column = Ident.col(field.name)
  def col[Doc <: Document[Doc], V](qualifier: String, field: Field[Doc, V]): Ident.Column = Ident.col(qualifier, field.name)

  def arg(value: Any): Value = Value.Arg(value)
  def rawValue(sql: String): Value = Value.Raw(sql)
  def rawExpr(sql: String): Expr = Expr.Raw(sql)

  def asc(col: Ident.Column): OrderBy = OrderBy(col, SortDirection.Asc)
  def desc(col: Ident.Column): OrderBy = OrderBy(col, SortDirection.Desc)

  implicit final class TableOps(private val t: Ident.Table) extends AnyVal {
    /** Qualified column using this table's alias if present, otherwise its name. */
    def col[Doc <: Document[Doc], V](field: Field[Doc, V]): Ident.Column = Ident.col(t.ref, field.name)
    def col(name: String): Ident.Column = Ident.col(t.ref, name)
  }

  // Column ops
  implicit final class ColumnOps(private val c: Ident.Column) extends AnyVal {
    def ===(value: Any): Expr = Expr.Eq(c, Expr.Term.value(Value.Arg(value)))
    def ===(other: Ident.Column): Expr = Expr.Eq(c, Expr.Term.col(other))
    def =!=(value: Any): Expr = Expr.Neq(c, Expr.Term.value(Value.Arg(value)))
    def =!=(other: Ident.Column): Expr = Expr.Neq(c, Expr.Term.col(other))
    def <(value: Any): Expr = Expr.Lt(c, Expr.Term.value(Value.Arg(value)))
    def <(other: Ident.Column): Expr = Expr.Lt(c, Expr.Term.col(other))
    def <=(value: Any): Expr = Expr.Lte(c, Expr.Term.value(Value.Arg(value)))
    def <=(other: Ident.Column): Expr = Expr.Lte(c, Expr.Term.col(other))
    def >(value: Any): Expr = Expr.Gt(c, Expr.Term.value(Value.Arg(value)))
    def >(other: Ident.Column): Expr = Expr.Gt(c, Expr.Term.col(other))
    def >=(value: Any): Expr = Expr.Gte(c, Expr.Term.value(Value.Arg(value)))
    def >=(other: Ident.Column): Expr = Expr.Gte(c, Expr.Term.col(other))
    def like(pattern: Any): Expr = Expr.Like(c, Value.Arg(pattern))
    def isNull: Expr = Expr.IsNull(c)
    def isNotNull: Expr = Expr.IsNotNull(c)
    def in(values: Any*): Expr = Expr.In(c, values.toList.map(Value.Arg))
  }
}


