package lightdb.store

import lightdb.Id

case class ObjectData[T](id: Id[T], data: Array[Byte])
