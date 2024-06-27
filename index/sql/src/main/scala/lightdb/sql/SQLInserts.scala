package lightdb.sql

import lightdb.document.Document
import lightdb.index.Index

import java.sql.PreparedStatement

class SQLInserts[D <: Document[D]](ps: PreparedStatement,
                                   indexes: List[Index[_, D]],
                                   maxBatch: Int) {
  private var batchSize = 0

  def insert(doc: D): Unit = {
    indexes.map(_.getJson(doc)).zipWithIndex.foreach {
      case (value, index) => SQLIndexer.setValue(ps, index + 1, value)
    }
    ps.addBatch()
    synchronized {
      batchSize += 1
      if (batchSize >= maxBatch) {
        ps.executeBatch()
        batchSize = 0
      }
    }
  }

  def execute(): Unit = synchronized {
    if (batchSize > 0) {
      ps.executeBatch()
      batchSize = 0
    }
  }

  def close(): Unit = ps.close()
}
