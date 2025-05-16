package lightdb.graph

import fabric._
import fabric.define.DefType
import fabric.rw._
import lightdb.doc.Document
import lightdb.id.{EdgeId, Id}

final case class ReverseEdgeDocument[
  E <: EdgeDocument[E, F, T],
  F <: Document[F],
  T <: Document[T]
](edge: E, _from: Id[T], _to: Id[F], _id: EdgeId[ReverseEdgeDocument[E, F, T], T, F]) extends EdgeDocument[ReverseEdgeDocument[E, F, T], T, F]

object ReverseEdgeDocument {
  implicit def rw[
    E <: EdgeDocument[E, F, T],
    F <: Document[F],
    T <: Document[T]
  ](implicit erw: RW[E]): RW[ReverseEdgeDocument[E, F, T]] = RW.from[ReverseEdgeDocument[E, F, T]](
    r = doc => obj(
      "edge" -> doc.edge.json,
      "_from" -> doc._from.json,
      "_to" -> doc._to.json,
      "_id" -> doc._id.json
    ),
    w = json => ReverseEdgeDocument(
      edge = json("edge").as[E],
      _from = json("_from").as[Id[T]],
      _to = json("_to").as[Id[F]],
      _id = json("_id").as[EdgeId[ReverseEdgeDocument[E, F, T], T, F]]
    ),
    d = DefType.Obj(
      className = Some("lightdb.graph.ReverseEdgeDocument"),
      "edge" -> erw.definition,
      "_from" -> DefType.Str,
      "_to" -> DefType.Str,
      "_id" -> DefType.Str
    )
  )

  def apply[
    E <: EdgeDocument[E, F, T],
    F <: Document[F],
    T <: Document[T]
  ](edge: E): ReverseEdgeDocument[E, F, T] = new ReverseEdgeDocument[E, F, T](
    edge = edge,
    _from = edge._to,
    _to = edge._from,
    _id = EdgeId(edge._to, edge._from)
  )

  def createModel[E <: EdgeDocument[E, F, T], F <: Document[F], T <: Document[T]](name: String)(implicit rw: RW[E]): EdgeModel[ReverseEdgeDocument[E, F, T], T, F] =
    ReverseEdgeModel[E, F, T](name)
}