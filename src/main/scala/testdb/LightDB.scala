package testdb

import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}

class LightDB(val store: ObjectStore) {
  def collection[T](implicit dataManager: DataManager[T]): Collection[T] = new Collection[T](this, dataManager)

  def dispose(): Unit = store.dispose()
}

class Collection[T] private[testdb](db: LightDB, dataManager: DataManager[T]) {
  private implicit def executionContext: ExecutionContext = db.store.executionContext

  def get(id: Id[T]): Future[Option[T]] = db.store.get(id).map(_.map(dataManager.fromArray))
  def put(id: Id[T], value: T): Future[T] = db.store.put(id, dataManager.toArray(value)).map(_ => value)
  def modify(id: Id[T])(f: Option[T] => Option[T]): Future[Option[T]] = {
    var result: Option[T] = None
    db.store.modify(id) { bytes =>
      val value = bytes.map(dataManager.fromArray)
      result = f(value)
      result.map(dataManager.toArray)
    }.map(_ => result)
  }
  def delete(id: Id[T]): Future[Unit] = db.store.delete(id)
}

trait DataManager[T] {
  def fromArray(array: Array[Byte]): T
  def toArray(value: T): Array[Byte]
}

case class Stored(`type`: StoredType, bb: ByteBuffer, values: Map[String, StoredValue]) {
  def apply[T](name: String): T = {
    val sv = values(name)
    sv.`type`.read(sv.offset, bb).asInstanceOf[T]
  }
}

case class StoredValue(offset: Int, `type`: ValueType[_])

case class StoredType(types: Vector[ValueTypeEntry]) {
  def create(tuples: (String, Any)*): Array[Byte] = {
    assert(tuples.length == types.length, "Supplied tuples must be identical to the types")
    val map = tuples.toMap
    val entriesAndValues = types.map(e => (e, map(e.name)))
    val length = entriesAndValues.foldLeft(0)((sum, t) => sum + t._1.`type`.asInstanceOf[ValueType[Any]].length(t._2))
    val bb = ByteBuffer.allocate(length)
    entriesAndValues.foreach {
      case (e, v) => e.`type`.asInstanceOf[ValueType[Any]].write(bb, v)
    }
    bb.flip()
    bb.array()
  }

  def apply(bytes: Array[Byte]): Stored = {
    val bb = ByteBuffer.wrap(bytes)
    var offset = 0
    var values = Map.empty[String, StoredValue]
    types.foreach { e =>
      val length = e.`type`.length(0, bb)
      val sv = StoredValue(offset, e.`type`)
      values += e.name -> sv
      offset += length
    }
    Stored(this, bb, values)
  }
}

case class ValueTypeEntry(name: String, `type`: ValueType[_])

trait ValueType[V] {
  def read(offset: Int, bytes: ByteBuffer): V
  def write(bytes: ByteBuffer, value: V): Unit
  def length(value: V): Int
  def length(offset: Int, bytes: ByteBuffer): Int
}

object StringType extends ValueType[String] {
  override def read(offset: Int, bytes: ByteBuffer): String = {
    val length = bytes.getInt(offset)
    if (length == 4) {
      ""
    } else {
      bytes.getInt
      val array = new Array[Byte](length)

      (0 until length).foreach { i =>
        val b = bytes.get(offset + 4 + i)
        array(i) = b
      }
      new String(array, "UTF-8")
    }
  }

  override def length(offset: Int, bytes: ByteBuffer): Int = bytes.getInt(offset) + 4

  override def write(bytes: ByteBuffer, value: String): Unit = {
    bytes.putInt(value.length)
    if (value.nonEmpty) {
      bytes.put(value.getBytes("UTF-8"))
    }
  }

  override def length(value: String): Int = (value.length + 1) * 4
}

object IntType extends ValueType[Int] {
  override def read(offset: Int, bytes: ByteBuffer): Int = bytes.getInt(offset)

  override def write(bytes: ByteBuffer, value: Int): Unit = bytes.putInt(value)

  override def length(value: Int): Int = 4

  override def length(offset: Int, bytes: ByteBuffer): Int = 4
}

case class Person(name: String, age: Int, location: Location)
case class Location(city: String, state: String)