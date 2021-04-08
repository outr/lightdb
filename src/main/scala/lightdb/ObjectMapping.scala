package lightdb

import lightdb.data.DataManager
import lightdb.field.{Field, FieldFeature}
import lightdb.index.Indexer

trait ObjectMapping[D <: Document[D]] {
  def fields: List[Field[D, _]]

  def dataManager: DataManager[D]

  def indexer: Indexer[D]

  def collectionName: String

  def field[F](name: String, features: FieldFeature*): Field[D, F] = {
    Field[D, F](name, features.toList)
  }
}
