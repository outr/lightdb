package lightdb.data.stored

import lightdb.data.DataManager

class StoredDataManager(`type`: StoredType) extends DataManager[Stored] {
  override def fromArray(array: Array[Byte]): Stored = `type`(array)

  override def toArray(value: Stored): Array[Byte] = value.bb.array()
}