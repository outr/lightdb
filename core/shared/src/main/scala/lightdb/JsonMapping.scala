package lightdb

import fabric.rw.ReaderWriter
import lightdb.data.{DataManager, JsonDataManager}

trait JsonMapping[D <: Document[D]] extends ObjectMapping[D] {
  implicit val rw: ReaderWriter[D]

  override def dataManager: DataManager[D] = JsonDataManager[D]()
}