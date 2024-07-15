package lightdb.sql

import lightdb.feature.DBFeatureKey
import lightdb.sql.connect.ConnectionManager

final case class SQLDatabase(connectionManager: ConnectionManager)

object SQLDatabase {
  val Key: DBFeatureKey[SQLDatabase] = DBFeatureKey("sqlDatabase")
}