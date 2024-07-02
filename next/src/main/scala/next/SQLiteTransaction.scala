package next

import java.sql.{PreparedStatement, ResultSet, Statement}
import scala.util.Try

class SQLiteTransaction[Doc] extends Transaction[Doc] {
  private[next] var ps: PreparedStatement = _
  private[next] var batch: Int = 0
  private var statements = List.empty[Statement]
  private var resultSets = List.empty[ResultSet]

  private[next] var cache = Map.empty[String, PreparedStatement]

  def register(s: Statement): Unit = synchronized {
    statements = (s :: statements).distinct
  }

  def register(rs: ResultSet): Unit = synchronized {
    resultSets = rs :: resultSets
  }

  def close(): Unit = {
    if (batch > 0) {
      ps.executeBatch()
    }
    resultSets.foreach(rs => Try(rs.close()))
    statements.foreach(s => Try(s.close()))
    if (ps != null) ps.close()
  }
}
