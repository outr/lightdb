package lightdb.doc

sealed trait DocState[Doc <: Document[Doc]] {
  def doc: Doc
}

object DocState {
  case class Added[Doc <: Document[Doc]](doc: Doc) extends DocState[Doc]
  case class Modified[Doc <: Document[Doc]](doc: Doc) extends DocState[Doc]
  case class Removed[Doc <: Document[Doc]](doc: Doc) extends DocState[Doc]
}