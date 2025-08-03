package lightdb.sql.query

import lightdb.util.ActionIterator
import rapid.Opt

import java.sql.ResultSet

class ResultSetIterator private(rs: ResultSet) extends Iterator[ResultSet] {
  private var hasNextOption: Opt[Boolean] = Opt.Empty

  override def hasNext: Boolean = hasNextOption match {
    case Opt.Value(b) => b
    case Opt.Empty =>
      val b = rs.next()
      hasNextOption = Opt(b)
      b
  }

  override def next(): ResultSet = try {
    rs
  } finally {
    hasNextOption = Opt.Empty
  }
}

object ResultSetIterator {
  def apply(rs: ResultSet, onClose: => Unit): Iterator[ResultSet] = {
    val rsi = new ResultSetIterator(rs)
    ActionIterator(rsi, onClose = () => onClose)
  }
}