package lightdb

import lightdb.data.DataManager
import lightdb.field.Field

trait ObjectMapping[D <: Document[D]] {
  def fields: List[Field[D, _]]

  def dataManager: DataManager[D]

  def field[F](name: String, getter: D => F): Field[D, F] = {
    Field[D, F](name, getter, Nil)
  }
}