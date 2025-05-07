package lightdb.id

import fabric.rw._
import lightdb.doc.Document
import lightdb.graph.EdgeDocument

case class EdgeId[Doc <: EdgeDocument[Doc, From, To], From <: Document[From], To <: Document[To]](from: Id[From],
                                                                                                  to: Id[To],
//                                                                                                  prefixes: Set[String],    // TODO: Do this?
                                                                                                  extra: Option[String]) extends Id[Doc] {
  override lazy val value: String = s"${from.value}-${to.value}${extra.map(s => s"-$s").getOrElse("")}"
}

object EdgeId {
  private lazy val _rw: RW[EdgeId[_, _, _]] = RW.string(
    asString = _.value,
    fromString = parse
  )

  implicit def rw[D <: EdgeDocument[D, F, T], F <: Document[F], T <: Document[T]]: RW[EdgeId[D, F, T]] =
    _rw.asInstanceOf[RW[EdgeId[D, F, T]]]

  private val RegexFull = """(.+)-(.+)-(.+)""".r
  private val RegexPartial = """(.+)-(.+)""".r

  def apply[Doc <: EdgeDocument[Doc, From, To], From <: Document[From], To <: Document[To]](from: Id[From],
                                                                                            to: Id[To],
                                                                                            extra: String): EdgeId[Doc, From, To] = new EdgeId[Doc, From, To](from, to, Some(extra))

  def apply[Doc <: EdgeDocument[Doc, From, To], From <: Document[From], To <: Document[To]](from: Id[From],
                                                                                            to: Id[To]): EdgeId[Doc, From, To] = new EdgeId[Doc, From, To](from, to, None)

  def parse[Doc <: EdgeDocument[Doc, From, To], From <: Document[From], To <: Document[To]](value: String): EdgeId[Doc, From, To] = value match {
    case RegexFull(from, to, extra) => EdgeId[Doc, From, To](
      from = StringId[From](from),
      to = StringId[To](to),
      extra = Some(extra)
    )
    case RegexPartial(from, to) => EdgeId[Doc, From, To](
      from = StringId[From](from),
      to = StringId[To](to),
      extra = None
    )
  }
}