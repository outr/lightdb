package lightdb

import fabric.rw.RW
import lightdb.data.{DataManager, JsonDataManager}

trait JsonMapping[D <: Document[D]] extends ObjectMapping[D] {
  implicit val rw: RW[D]

  override def dataManager: DataManager[D] = JsonDataManager[D]()
}