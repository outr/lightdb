package lightdb.view

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.store.Store

/** Start a view query from a collection (alias defaults to the store name; pass an explicit alias
  * for self-joins or to disambiguate). */
def from[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: Store[Doc, Model]): RelQuery1[Doc, Model] =
  RelQuery1(store, store.name)

def from[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: Store[Doc, Model], alias: String): RelQuery1[Doc, Model] =
  RelQuery1(store, alias)

/** Reuse a finished sub-relation as an aliased join source (a derived table); its output columns
  * are addressed through `model`'s fields. */
def derived[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model, alias: String)(relation: Relation): Derived[Doc, Model] =
  Derived(relation, model, alias)

/** A literal value expression. */
def lit(value: Any): Expr = Expr.Lit(value)

/** String concatenation of the parts (e.g. to compose a deterministic view `_id`). */
def concat(parts: Expr*): Expr = Expr.Func("CONCAT", parts.toList)

/** Combine multiple view-query arms (each a finished `select`) into one result. */
def union(relations: Relation*): Relation = Relation.Union(relations.toList)

/** Aggregates (valid in a grouped `select`). */
def count(expr: Expr): Expr = Expr.Agg(AggOp.Count, expr)
def max(expr: Expr): Expr = Expr.Agg(AggOp.Max, expr)
def min(expr: Expr): Expr = Expr.Agg(AggOp.Min, expr)
def sum(expr: Expr): Expr = Expr.Agg(AggOp.Sum, expr)

/** A searched CASE: `caseWhen(cond -> value, …).otherwise(value)`. */
def caseWhen(branches: (Cond, Expr)*): CaseBuilder = CaseBuilder(branches.toList)

final case class CaseBuilder(branches: List[(Cond, Expr)]) {
  def when(cond: Cond, value: Expr): CaseBuilder = CaseBuilder(branches :+ (cond -> value))
  def otherwise(value: Expr): Expr = Expr.Case(branches, value)
}

/** Bind a view-model field to a source expression: `Adult.name := p(Person.name)`. */
extension [Doc <: Document[Doc], V](field: Field[Doc, V])
  def :=(expr: Expr): Binding = Binding(field.name, expr)

extension (e: Expr) {
  def ===(value: Any): Cond = Cond.Cmp("=", e, toExpr(value))
  def !==(value: Any): Cond = Cond.Cmp("<>", e, toExpr(value))
  def >(value: Any): Cond = Cond.Cmp(">", e, toExpr(value))
  def >=(value: Any): Cond = Cond.Cmp(">=", e, toExpr(value))
  def <(value: Any): Cond = Cond.Cmp("<", e, toExpr(value))
  def <=(value: Any): Cond = Cond.Cmp("<=", e, toExpr(value))
}

private def toExpr(value: Any): Expr = value match {
  case e: Expr => e
  case other => Expr.Lit(other)
}
