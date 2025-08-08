package lightdb.sql.query

import lightdb.util.ActionIterator

import java.sql.ResultSet

class ResultSetIterator private(rs: ResultSet) extends Iterator[ResultSet] {
  private var hasNextOption: Option[Boolean] = None

  override def hasNext: Boolean = hasNextOption match {
    case Some(b) => b
    case None =>
      val b = rs.next()
      hasNextOption = Some(b)
      b
  }

  override def next(): ResultSet = try {
    rs
  } finally {
    hasNextOption = None
  }
}

object ResultSetIterator {
  def apply(rs: ResultSet, onClose: => Unit): Iterator[ResultSet] = {
    val rsi = new ResultSetIterator(rs)
    ActionIterator(rsi, onClose = () => onClose)
  }
}