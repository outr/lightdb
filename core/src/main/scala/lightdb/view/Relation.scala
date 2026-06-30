package lightdb.view

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.store.Store

/**
 * Backend-agnostic, introspectable relational IR for a [[View]]. A `Relation` both *executes*
 * (lowered to SQL, or interpreted on non-SQL backends) and *describes* a query, so the view
 * engine can derive its dependencies and incremental-maintenance scopes from the structure
 * itself rather than from hand-declared metadata.
 *
 * Built with a typed builder ([[from]]) over model [[Field]]s, so column references are
 * refactor-safe and output is bound to the view's model via `field := expr`.
 */
sealed trait Relation {
  /** Every collection scanned anywhere in this relation (including subqueries) — the view's
    * dependency set, used to auto-register incremental-maintenance triggers. */
  def dependencies: Set[Store[?, ?]]
}

object Relation {
  final case class Scan(store: Store[?, ?], alias: String) extends Relation {
    override def dependencies: Set[Store[?, ?]] = Set(store)
  }

  final case class Filter(source: Relation, cond: Cond) extends Relation {
    override def dependencies: Set[Store[?, ?]] = source.dependencies ++ cond.dependencies
  }

  final case class Project(source: Relation, bindings: List[Binding]) extends Relation {
    override def dependencies: Set[Store[?, ?]] = source.dependencies ++ bindings.flatMap(_.expr.dependencies)
  }

  final case class Join(left: Relation, right: Relation, joinType: JoinType, on: Cond) extends Relation {
    override def dependencies: Set[Store[?, ?]] = left.dependencies ++ right.dependencies ++ on.dependencies
  }

  final case class Aggregate(source: Relation, groupBy: List[Expr], bindings: List[Binding]) extends Relation {
    override def dependencies: Set[Store[?, ?]] =
      source.dependencies ++ groupBy.flatMap(_.dependencies) ++ bindings.flatMap(_.expr.dependencies)
  }

  final case class Union(arms: List[Relation]) extends Relation {
    override def dependencies: Set[Store[?, ?]] = arms.flatMap(_.dependencies).toSet
  }

  /** A sub-relation reused as an aliased source (a derived table) — its output rows exposed under
    * `alias` so an outer query can join/reference them. */
  final case class Derived(source: Relation, alias: String) extends Relation {
    override def dependencies: Set[Store[?, ?]] = source.dependencies
  }
}

enum JoinType {
  case Inner, Left
}

/** A typed value expression in the relational IR. */
sealed trait Expr {
  def dependencies: Set[Store[?, ?]] = Set.empty
}

object Expr {
  /** A column reference, qualified by its source's alias. Built via [[Source.apply]] from a
    * typed [[Field]], so the name is refactor-safe. */
  final case class Col(alias: String, fieldName: String) extends Expr
  /** A literal, parameterized when rendered. */
  final case class Lit(value: Any) extends Expr
  /** A function call `name(args…)`. */
  final case class Func(name: String, args: List[Expr]) extends Expr {
    override def dependencies: Set[Store[?, ?]] = args.flatMap(_.dependencies).toSet
  }
  /** A cast `CAST(expr AS sqlType)`. */
  final case class Cast(expr: Expr, sqlType: String) extends Expr {
    override def dependencies: Set[Store[?, ?]] = expr.dependencies
  }
  /** An aggregate over a group (valid only in an [[Relation.Aggregate]]'s bindings). */
  final case class Agg(op: AggOp, expr: Expr) extends Expr {
    override def dependencies: Set[Store[?, ?]] = expr.dependencies
  }
  /** A searched CASE: the first branch whose condition holds, else `orElse`. */
  final case class Case(whens: List[(Cond, Expr)], orElse: Expr) extends Expr {
    override def dependencies: Set[Store[?, ?]] =
      whens.flatMap { case (c, e) => c.dependencies ++ e.dependencies }.toSet ++ orElse.dependencies
  }
}

enum AggOp {
  case Count, Max, Min, Sum
}

/** A boolean condition (WHERE / join predicate). */
sealed trait Cond {
  def dependencies: Set[Store[?, ?]] = Set.empty
  def and(other: Cond): Cond = Cond.And(this, other)
  def or(other: Cond): Cond = Cond.Or(this, other)
  def unary_! : Cond = Cond.Not(this)
}

object Cond {
  final case class Cmp(op: String, left: Expr, right: Expr) extends Cond {
    override def dependencies: Set[Store[?, ?]] = left.dependencies ++ right.dependencies
  }
  final case class And(left: Cond, right: Cond) extends Cond {
    override def dependencies: Set[Store[?, ?]] = left.dependencies ++ right.dependencies
  }
  final case class Or(left: Cond, right: Cond) extends Cond {
    override def dependencies: Set[Store[?, ?]] = left.dependencies ++ right.dependencies
  }
  final case class Not(cond: Cond) extends Cond {
    override def dependencies: Set[Store[?, ?]] = cond.dependencies
  }
}

/** An output column: the view-model field's name bound to a source expression. */
final case class Binding(name: String, expr: Expr)
