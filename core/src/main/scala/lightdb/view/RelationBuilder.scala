package lightdb.view

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.store.Store

/**
 * A typed handle for one side of a view query, scoped to a source's alias. Column references go
 * through it so they're typed to that source's model and carry the right alias (disambiguating
 * self-joins and keeping clear of the single-collection [[lightdb.filter.Filter]] DSL).
 */
final class Side[Doc <: Document[Doc], Model <: DocumentModel[Doc]] private[view] (alias: String) {
  def apply[V](field: Field[Doc, V]): Expr = Expr.Col(alias, field.name)
}

/** Anything that can be a join source: a collection ([[RelQuery1]]) or a sub-relation ([[Derived]]). */
sealed trait QuerySource[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
  private[view] def relation: Relation
  private[view] def alias: String
  private[view] def side: Side[Doc, Model]

  /** Inner-join another source; the `on` condition references both sides' typed handles. */
  def join[D2 <: Document[D2], M2 <: DocumentModel[D2]](right: QuerySource[D2, M2])
                                                      (on: (Side[Doc, Model], Side[D2, M2]) => Cond): RelQuery2[Doc, Model, D2, M2] =
    RelQuery2(this, right, JoinType.Inner, on(side, right.side))

  /** Left-outer-join another source (unmatched right columns read as null). */
  def leftJoin[D2 <: Document[D2], M2 <: DocumentModel[D2]](right: QuerySource[D2, M2])
                                                          (on: (Side[Doc, Model], Side[D2, M2]) => Cond): RelQuery2[Doc, Model, D2, M2] =
    RelQuery2(this, right, JoinType.Left, on(side, right.side))
}

/**
 * A join-capable query over a single collection — the root of the view query builder. `.join` adds
 * another source; `.where`/`.groupBy`/`.select` reference fields through a typed [[Side]] handle.
 */
final case class RelQuery1[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: Store[Doc, Model],
                                                                             alias: String,
                                                                             filter: Option[Cond] = None)
  extends QuerySource[Doc, Model] {
  private[view] val side: Side[Doc, Model] = new Side(alias)

  private[view] def relation: Relation = {
    val scan: Relation = Relation.Scan(store, alias)
    filter.map(Relation.Filter(scan, _)).getOrElse(scan)
  }

  def where(f: Side[Doc, Model] => Cond): RelQuery1[Doc, Model] =
    copy(filter = Some(filter.map(_.and(f(side))).getOrElse(f(side))))

  def groupBy(f: Side[Doc, Model] => Seq[Expr]): GroupedQuery1[Doc, Model] =
    GroupedQuery1(relation, side, f(side).toList)

  def select(f: Side[Doc, Model] => Seq[Binding]): Relation =
    Relation.Project(relation, f(side).toList)
}

/** A sub-relation reused as a join source (a derived table). Its output columns are the inner
  * relation's bindings, addressed through `model`'s fields. */
final case class Derived[Doc <: Document[Doc], Model <: DocumentModel[Doc]](inner: Relation,
                                                                           model: Model,
                                                                           alias: String)
  extends QuerySource[Doc, Model] {
  private[view] val side: Side[Doc, Model] = new Side(alias)
  private[view] def relation: Relation = Relation.Derived(inner, alias)
}

/** A single-source query grouped for aggregation; `.select` binds group keys + aggregates. */
final case class GroupedQuery1[Doc <: Document[Doc], Model <: DocumentModel[Doc]](source: Relation,
                                                                                 side: Side[Doc, Model],
                                                                                 keys: List[Expr]) {
  def select(f: Side[Doc, Model] => Seq[Binding]): Relation =
    Relation.Aggregate(source, keys, f(side).toList)
}

/** A view query over two joined sources. `.where` adds a post-join condition; `.groupBy`/`.select`
  * finalize, referencing both sources through their typed [[Side]] handles. */
final case class RelQuery2[D1 <: Document[D1], M1 <: DocumentModel[D1], D2 <: Document[D2], M2 <: DocumentModel[D2]](
    left: QuerySource[D1, M1],
    right: QuerySource[D2, M2],
    joinType: JoinType,
    on: Cond,
    filter: Option[Cond] = None) {

  private[view] def joinedSource: Relation = {
    val joined = Relation.Join(left.relation, right.relation, joinType, on)
    filter.map(Relation.Filter(joined, _)).getOrElse(joined)
  }

  def where(f: (Side[D1, M1], Side[D2, M2]) => Cond): RelQuery2[D1, M1, D2, M2] = {
    val c = f(left.side, right.side)
    copy(filter = Some(filter.map(_.and(c)).getOrElse(c)))
  }

  def groupBy(f: (Side[D1, M1], Side[D2, M2]) => Seq[Expr]): GroupedQuery2[D1, M1, D2, M2] =
    GroupedQuery2(joinedSource, left.side, right.side, f(left.side, right.side).toList)

  def select(f: (Side[D1, M1], Side[D2, M2]) => Seq[Binding]): Relation =
    Relation.Project(joinedSource, f(left.side, right.side).toList)
}

/** A two-source joined query grouped for aggregation; `.select` binds group keys + aggregates. */
final case class GroupedQuery2[D1 <: Document[D1], M1 <: DocumentModel[D1], D2 <: Document[D2], M2 <: DocumentModel[D2]](
    source: Relation,
    leftSide: Side[D1, M1],
    rightSide: Side[D2, M2],
    keys: List[Expr]) {
  def select(f: (Side[D1, M1], Side[D2, M2]) => Seq[Binding]): Relation =
    Relation.Aggregate(source, keys, f(leftSide, rightSide).toList)
}
