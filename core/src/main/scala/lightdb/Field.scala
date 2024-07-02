package lightdb

import fabric.rw.RW

sealed trait Field[Doc, V] {
  def rw: RW[V]

  def name: String
  def get: Doc => V

  def ===(value: V): Filter[Doc] = Filter.Equals(this, value)
}

object Field {
  case class Basic[Doc, V](name: String, get: Doc => V)(implicit val rw: RW[V]) extends Field[Doc, V]
  case class Index[Doc, V](name: String, get: Doc => V)(implicit val rw: RW[V]) extends Field[Doc, V]
  case class Unique[Doc, V](name: String, get: Doc => V)(implicit val rw: RW[V]) extends Field[Doc, V]
}