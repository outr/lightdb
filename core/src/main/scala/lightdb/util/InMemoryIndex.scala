package lightdb.util

import lightdb.{Field, Id, Indexed}
import lightdb.doc.Document

import java.util.Comparator
import java.util.concurrent.ConcurrentHashMap

class InMemoryIndex[Doc <: Document[Doc], V](field: Indexed[Doc, V], comparator: Option[Comparator[V]]) {
  private val map = new ConcurrentHashMap[V, List[Id[Doc]]]
  private val currentValue = new ConcurrentHashMap[Id[Doc], V]
  private val sorted = new AtomicList[V](comparator)

  def set(doc: Doc): Unit = set(doc._id, field.get(doc))

  def set(id: Id[Doc], value: V): Unit = {
    Option(currentValue.get(id)).foreach(previous => remove(id, previous))
    map.compute(value, (_, list) => id :: list)
    currentValue.put(id, value)
    sorted.add(value)
  }

  def remove(id: Id[Doc], value: V): Unit = {
    map.computeIfPresent(value, (_, list) => list.filterNot(_ == id) match {
      case Nil => null
      case l => l
    })
    currentValue.remove(id)
    sorted.remove(value)
  }

  def clear(): Unit = {
    map.clear()
    currentValue.clear()
    sorted.clear()
  }

  def apply(value: V): List[Id[Doc]] = map.getOrDefault(value, Nil)

  def ascending: Iterator[V] = sorted.iterator
  def descending: Iterator[V] = sorted.reverseIterator
}