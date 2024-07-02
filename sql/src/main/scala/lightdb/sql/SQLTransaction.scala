package lightdb.sql

import lightdb.Transaction
import lightdb.sql.connect.ConnectionManager

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import scala.util.Try

case class SQLTransaction[Doc](connectionManager: ConnectionManager[Doc]) extends Transaction[Doc] {
  private[sql] var connection: Connection = _
  private[sql] var ps: PreparedStatement = _
  private[sql] var batch: Int = 0
  private var statements = List.empty[Statement]
  private var resultSets = List.empty[ResultSet]

  private[sql] var cache = Map.empty[String, PreparedStatement]

  def register(s: Statement): Unit = synchronized {
    statements = (s :: statements).distinct
  }

  def register(rs: ResultSet): Unit = synchronized {
    resultSets = rs :: resultSets
  }

  override def commit(): Unit = if (connection != null) {
    connection.commit()
  }

  override def rollback(): Unit = if (connection != null) {
    connection.rollback()
  }

  def close(): Unit = {
    if (batch > 0) {
      ps.executeBatch()
    }
    resultSets.foreach(rs => Try(rs.close()))
    statements.foreach(s => Try(s.close()))
    if (ps != null) ps.close()
    connectionManager.releaseConnection(this)
  }
}
