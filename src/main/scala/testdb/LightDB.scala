package testdb

import com.oath.halodb.{HaloDB, HaloDBOptions}
import io.youi.Unique

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ForkJoinPool}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, Promise}
import scala.util.matching.Regex

class LightDB(directory: String = "lightdb",
              processingThreads: Int = 32,
              indexThreads: Int = 8) {
  private val halo = {
    val opts = new HaloDBOptions
    opts.setBuildIndexThreads(indexThreads)

    HaloDB.open(directory, opts)
  }
  private lazy val locks: ConcurrentHashMap[String, Future[Unit]] = new ConcurrentHashMap[String, Future[Unit]]
  private implicit lazy val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(new ForkJoinPool(processingThreads))

  def get[T](id: Id[T]): Future[Option[Array[Byte]]] = Future {
    Option(halo.get(id.bytes))
  }

  def put[T](id: Id[T], value: Array[Byte]): Future[Array[Byte]] = Future {
    halo.put(id.bytes, value)
  }.map(_ => value)

  def delete[T](id: Id[T]): Future[Unit] = Future {
    halo.delete(id.bytes)
  }

  def modify[T](id: Id[T])(f: Option[Array[Byte]] => Option[Array[Byte]]): Future[Option[Array[Byte]]] = {
    val promise = Promise[Option[Array[Byte]]]
    locks.compute(id.toString, (_, future) => {
      val previous = Option(future).getOrElse(Future.successful(()))
      val next = previous.transformWith { _ =>
        val nextFuture = get[T](id).flatMap { current =>
          f(current) match {
            case None if current.isEmpty => Future.successful(None)               // No value
            case None => delete[T](id).map(_ => None)                             // Delete existing value
            case Some(updated) => put[T](id, updated).map(array => Some(array))   // Set new value
          }
        }
        nextFuture.failed.foreach { t =>
          // TODO: better error handling
          scribe.error(s"Failed to modify $id", t)
        }
        nextFuture
      }
      promise.completeWith(next)
      next.map(_ => ())
    })
    promise.future
  }

  def dispose(): Unit = halo.close()
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
    println(s"Length: $length")
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

/*

offset
0
 */

case class Person(name: String, age: Int, location: Location)
case class Location(city: String, state: String)