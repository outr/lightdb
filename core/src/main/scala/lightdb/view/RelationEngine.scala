package lightdb.view

import fabric.*
import rapid.Task

/**
 * Backend-agnostic executor for a [[Relation]]. Each [[Relation.Scan]] is read through its
 * collection's normal single-collection query, and joins / filters / projections are performed
 * here, so a view runs on any backend (including ones whose stores live in separate databases and
 * can't do a cross-collection SQL join). A SQL-native pushdown path can later short-circuit this
 * for co-located SQL stores, producing identical results.
 */
object RelationEngine {
  /** An in-flight row: the in-scope source objects, keyed by source alias. */
  private type Row = Map[String, Json]

  /**
   * Execute the relation, returning the output rows as JSON objects (decode to the view model).
   * Uses a backend's [[NativeViewExecutor]] when every dependency is co-located under one (e.g. a
   * single SQL database, which can JOIN them itself); otherwise, or when the native executor defers,
   * runs the portable in-memory engine. Both paths produce identical results.
   */
  def execute(relation: Relation): Task[List[Json]] = native(relation).getOrElse(generic(relation))

  private def native(relation: Relation): Option[Task[List[Json]]] = {
    val deps = relation.dependencies.toList
    val executors = deps.map(_.nativeViewExecutor)
    if (deps.nonEmpty && executors.forall(_.isDefined)) {
      val keys = executors.flatten.map(_.coLocationKey).distinct
      if (keys.size == 1) executors.head.flatMap(_.execute(relation)) else None
    } else {
      None
    }
  }

  private def generic(relation: Relation): Task[List[Json]] = relation match {
    case Relation.Project(source, bindings) =>
      eval(source).map(_.map(row => project(bindings, row)))
    case Relation.Aggregate(source, groupBy, bindings) =>
      eval(source).map(rows => aggregateRows(groupBy, bindings, rows))
    case Relation.Union(arms) =>
      arms.foldLeft(Task.pure(List.empty[Json]))((acc, arm) => acc.flatMap(l => generic(arm).map(l ++ _)))
    case Relation.Derived(source, _) =>
      generic(source)
    case other =>
      eval(other).map(_.map(row => row.values.headOption.getOrElse(obj())))
  }

  private def eval(rel: Relation): Task[List[Row]] = rel match {
    case Relation.Scan(store, alias) =>
      store.transaction(_.jsonStream.toList).map(_.map(j => Map(alias -> j)))
    case Relation.Filter(source, cond) =>
      eval(source).map(_.filter(row => matches(cond, row)))
    case Relation.Project(source, bindings) =>
      eval(source).map(_.map(row => Map("" -> project(bindings, row))))
    case Relation.Join(left, right, joinType, on) =>
      eval(left).flatMap(ls => eval(right).map(rs => join(ls, rs, joinType, on)))
    case Relation.Aggregate(source, groupBy, bindings) =>
      eval(source).map(rows => aggregateRows(groupBy, bindings, rows).map(j => Map("" -> j)))
    case Relation.Union(arms) =>
      arms.foldLeft(Task.pure(List.empty[Row]))((acc, arm) => acc.flatMap(l => eval(arm).map(l ++ _)))
    case Relation.Derived(source, alias) =>
      generic(source).map(_.map(j => Map(alias -> j)))
  }

  private def aggregateRows(groupBy: List[Expr], bindings: List[Binding], rows: List[Row]): List[Json] = {
    val groups = rows.groupBy(r => groupBy.map(g => value(g, r)))
    groups.values.toList.map(groupRows => obj(bindings.map(b => b.name -> groupValue(b.expr, groupRows))*))
  }

  private def groupValue(expr: Expr, rows: List[Row]): Json = expr match {
    case Expr.Agg(op, inner) => aggregate(op, rows.map(r => value(inner, r)))
    case Expr.Case(whens, orElse) =>
      whens.collectFirst { case (c, e) if groupMatches(c, rows) => groupValue(e, rows) }.getOrElse(groupValue(orElse, rows))
    case Expr.Cast(e, _) => groupValue(e, rows)
    case other => value(other, rows.head)
  }

  private def groupMatches(cond: Cond, rows: List[Row]): Boolean = cond match {
    case Cond.Cmp(op, l, r) => compare(op, groupValue(l, rows), groupValue(r, rows))
    case Cond.And(a, b) => groupMatches(a, rows) && groupMatches(b, rows)
    case Cond.Or(a, b) => groupMatches(a, rows) || groupMatches(b, rows)
    case Cond.Not(c) => !groupMatches(c, rows)
  }

  private def aggregate(op: AggOp, vals: List[Json]): Json = {
    val nonNull = vals.filter(_ != Null)
    op match {
      case AggOp.Count => NumInt(nonNull.size.toLong)
      case AggOp.Max => nonNull.reduceOption((a, b) => if (order(a, b) >= 0) a else b).getOrElse(Null)
      case AggOp.Min => nonNull.reduceOption((a, b) => if (order(a, b) <= 0) a else b).getOrElse(Null)
      case AggOp.Sum => NumDec(nonNull.flatMap(number).sum)
    }
  }

  private def join(left: List[Row], right: List[Row], joinType: JoinType, on: Cond): List[Row] = joinType match {
    case JoinType.Inner =>
      for {
        l <- left
        r <- right
        merged = l ++ r
        if matches(on, merged)
      } yield merged
    case JoinType.Left =>
      left.flatMap { l =>
        val matched = right.collect { case r if matches(on, l ++ r) => l ++ r }
        if (matched.nonEmpty) matched else List(l)
      }
  }

  private def project(bindings: List[Binding], row: Row): Json =
    obj(bindings.map(b => b.name -> value(b.expr, row))*)

  private def value(expr: Expr, row: Row): Json = expr match {
    case Expr.Col(alias, name) => row.get(alias).flatMap(_.get(name)).getOrElse(Null)
    case Expr.Lit(v) => toJson(v)
    case Expr.Cast(e, _) => value(e, row)
    case Expr.Func(name, args) => name.toUpperCase match {
      case "CONCAT" => Str(args.map(a => jsonString(value(a, row))).mkString)
      case _ => args.headOption.map(value(_, row)).getOrElse(Null)
    }
    case Expr.Case(whens, orElse) =>
      whens.collectFirst { case (c, e) if matches(c, row) => value(e, row) }.getOrElse(value(orElse, row))
    case Expr.Agg(_, inner) => value(inner, row)
  }

  private def matches(cond: Cond, row: Row): Boolean = cond match {
    case Cond.Cmp(op, l, r) => compare(op, value(l, row), value(r, row))
    case Cond.And(a, b) => matches(a, row) && matches(b, row)
    case Cond.Or(a, b) => matches(a, row) || matches(b, row)
    case Cond.Not(c) => !matches(c, row)
  }

  private def compare(op: String, a: Json, b: Json): Boolean = op match {
    case "=" => equal(a, b)
    case "<>" => !equal(a, b)
    case _ =>
      val c = order(a, b)
      op match {
        case "<" => c < 0
        case "<=" => c <= 0
        case ">" => c > 0
        case ">=" => c >= 0
        case _ => false
      }
  }

  private def equal(a: Json, b: Json): Boolean = (number(a), number(b)) match {
    case (Some(x), Some(y)) => x.compare(y) == 0
    case _ => a == b
  }

  private def order(a: Json, b: Json): Int = (number(a), number(b)) match {
    case (Some(x), Some(y)) => x.compare(y)
    case _ => (string(a), string(b)) match {
      case (Some(x), Some(y)) => x.compareTo(y)
      case _ => 0
    }
  }

  private def number(j: Json): Option[BigDecimal] = j match {
    case NumInt(l, _) => Some(BigDecimal(l))
    case NumDec(bd, _) => Some(bd)
    case _ => None
  }

  private def string(j: Json): Option[String] = j match {
    case Str(s, _) => Some(s)
    case _ => None
  }

  private def jsonString(j: Json): String = j match {
    case Str(s, _) => s
    case NumInt(l, _) => l.toString
    case NumDec(bd, _) => bd.toString
    case Bool(b, _) => b.toString
    case Null => ""
    case other => other.toString
  }

  private def toJson(v: Any): Json = v match {
    case j: Json => j
    case s: String => Str(s)
    case b: Boolean => Bool(b)
    case i: Int => NumInt(i.toLong)
    case l: Long => NumInt(l)
    case d: Double => NumDec(BigDecimal(d))
    case other => Str(other.toString)
  }
}
