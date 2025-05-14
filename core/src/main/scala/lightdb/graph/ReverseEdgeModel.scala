package lightdb.graph

import fabric.rw.RW
import lightdb.doc.{Document, JsonConversion}
import lightdb.field.Field

case class ReverseEdgeModel[Edge <: EdgeDocument[Edge, From, To], From <: Document[From], To <: Document[To]](name: String)(implicit erw: RW[Edge], val rw: RW[ReverseEdgeDocument[Edge, From, To]]) extends EdgeModel[ReverseEdgeDocument[Edge, From, To], To, From] with JsonConversion[ReverseEdgeDocument[Edge, From, To]] {
  val edge: Field[ReverseEdgeDocument[Edge, From, To], Edge] = field("edge", _.edge)
}