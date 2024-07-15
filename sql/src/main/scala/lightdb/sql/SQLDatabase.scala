package lightdb.sql

import lightdb.LightDB
import lightdb.sql.connect.ConnectionManager

trait SQLDatabase extends LightDB {
  def connectionManager: ConnectionManager
}