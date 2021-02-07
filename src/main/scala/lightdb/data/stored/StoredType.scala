package lightdb.data.stored

import java.nio.ByteBuffer

case class StoredType(types: Vector[ValueTypeEntry]) {
  def create(tuples: (String, Any)*): Stored = {
    assert(tuples.length == types.length, "Supplied tuples must be identical to the types")
    val map = tuples.toMap
    val entriesAndValues = types.map(e => (e, map(e.name)))
    var offsets = Map.empty[String, Int]
    val length = entriesAndValues.foldLeft(0)((sum, t) => {
      offsets += t._1.name -> sum
      sum + t._1.`type`.asInstanceOf[ValueType[Any]].length(t._2)
    })
    val bb = ByteBuffer.allocate(length)
    entriesAndValues.foreach {
      case (e, v) => e.`type`.asInstanceOf[ValueType[Any]].write(bb, v)
    }
    bb.flip()
    val values = types.map(t => t.name -> StoredValue(offsets(t.name), t.`type`)).toMap
    Stored(this, bb, values)
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