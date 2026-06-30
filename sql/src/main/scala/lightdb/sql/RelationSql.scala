package lightdb.sql

import lightdb.store.Store
import lightdb.view.*

/**
 * Lowers a backend-agnostic [[Relation]] to a single SQL `SELECT` so a co-located SQL database can
 * execute the whole view query (joins, aggregates, unions) with its own indexes, rather than the
 * generic engine streaming each collection into memory. Returns `None` for any shape it can't lower
 * (e.g. a [[Relation.Scan]] over a non-SQL store), so the caller falls back to the generic engine.
 *
 * Output columns are aliased to the view-model field names (quoted, so case is preserved), letting
 * `SQLStoreTransaction.sqlStream` decode each row straight into the view's document.
 */
object RelationSql {
  def lower(relation: Relation): Option[String] = select(relation)

  private final case class Source(from: String, wheres: List[String])

  private def select(relation: Relation): Option[String] = relation match {
    case Relation.Project(source, bindings) =>
      sourceOf(source).map { s =>
        s"SELECT ${columns(bindings)} FROM ${s.from}${whereClause(s.wheres)}"
      }
    case Relation.Aggregate(source, groupBy, bindings) =>
      sourceOf(source).map { s =>
        val by = groupBy.map(expr).mkString(", ")
        s"SELECT ${columns(bindings)} FROM ${s.from}${whereClause(s.wheres)} GROUP BY $by"
      }
    case Relation.Union(arms) =>
      val lowered = arms.map(select)
      if (lowered.forall(_.isDefined)) Some(lowered.flatten.mkString(" UNION ALL ")) else None
    case Relation.Derived(source, _) =>
      select(source)
    case _ =>
      None
  }

  private def sourceOf(relation: Relation): Option[Source] = relation match {
    case Relation.Scan(store, alias) =>
      table(store).map(t => Source(s"$t AS ${quote(alias)}", Nil))
    case Relation.Filter(source, cond) =>
      sourceOf(source).map(s => s.copy(wheres = s.wheres :+ condition(cond)))
    case Relation.Join(left, right, joinType, on) =>
      for {
        l <- sourceOf(left)
        r <- sourceOf(right)
      } yield joinType match {
        case JoinType.Inner =>
          Source(s"${l.from} JOIN ${r.from} ON (${condition(on)})", l.wheres ++ r.wheres)
        case JoinType.Left =>
          val onSql = (condition(on) :: r.wheres).mkString(" AND ")
          Source(s"${l.from} LEFT JOIN ${r.from} ON ($onSql)", l.wheres)
      }
    case Relation.Derived(inner, alias) =>
      select(inner).map(sql => Source(s"($sql) AS ${quote(alias)}", Nil))
    case _ =>
      None
  }

  private def table(store: Store[?, ?]): Option[String] = store match {
    case s: SQLStore[?, ?] => Some(s.fqn)
    case _ => None
  }

  private def columns(bindings: List[Binding]): String =
    bindings.map(b => s"${expr(b.expr)} AS ${quote(b.name)}").mkString(", ")

  private def whereClause(wheres: List[String]): String =
    if (wheres.isEmpty) "" else s" WHERE ${wheres.mkString(" AND ")}"

  private def expr(e: Expr): String = e match {
    case Expr.Col(alias, name) => s"${quote(alias)}.${quote(name)}"
    case Expr.Lit(value) => literal(value)
    case Expr.Cast(inner, sqlType) => s"CAST(${expr(inner)} AS $sqlType)"
    case Expr.Func(name, args) => s"$name(${args.map(expr).mkString(", ")})"
    case Expr.Agg(op, inner) => s"${aggFn(op)}(${expr(inner)})"
    case Expr.Case(whens, orElse) =>
      val branches = whens.map { case (c, r) => s"WHEN ${condition(c)} THEN ${expr(r)}" }.mkString(" ")
      s"CASE $branches ELSE ${expr(orElse)} END"
  }

  private def aggFn(op: AggOp): String = op match {
    case AggOp.Count => "COUNT"
    case AggOp.Max => "MAX"
    case AggOp.Min => "MIN"
    case AggOp.Sum => "SUM"
  }

  private def condition(cond: Cond): String = cond match {
    case Cond.Cmp(op, left, right) => s"(${expr(left)} $op ${expr(right)})"
    case Cond.And(a, b) => s"(${condition(a)} AND ${condition(b)})"
    case Cond.Or(a, b) => s"(${condition(a)} OR ${condition(b)})"
    case Cond.Not(c) => s"(NOT ${condition(c)})"
  }

  private def literal(value: Any): String = value match {
    case null => "NULL"
    case s: String => s"'${s.replace("'", "''")}'"
    case b: Boolean => if (b) "TRUE" else "FALSE"
    case n: Int => n.toString
    case n: Long => n.toString
    case n: Short => n.toString
    case n: Double => n.toString
    case n: Float => n.toString
    case n: BigDecimal => n.toString
    case other => s"'${other.toString.replace("'", "''")}'"
  }

  private def quote(name: String): String = SqlIdent.quote(name)
}
