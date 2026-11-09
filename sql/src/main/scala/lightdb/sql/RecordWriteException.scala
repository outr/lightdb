package lightdb.sql

import java.sql.SQLException

/**
 * A write batch that failed, named by the record it failed on rather than by its statement. The driver's exception is the cause,
 * and its SQL state and vendor code are kept.
 *
 * @param storeName the store written to
 * @param id the id of the record the batch failed on, when the driver says which
 * @param batchSize the number of records in the batch
 * @param column the column the database refused, when known
 * @param parameter the one-based statement parameter the database refused, when known
 * @param reason the database's own words
 */
class RecordWriteException(val storeName: String,
                           val id: Option[String],
                           val batchSize: Int,
                           val column: Option[String],
                           val parameter: Option[Int],
                           val reason: String,
                           cause: SQLException)
  extends SQLException(RecordWriteException.message(storeName, id, batchSize, column, parameter, reason), cause.getSQLState, cause.getErrorCode, cause)

object RecordWriteException {
  private def message(storeName: String,
                      id: Option[String],
                      batchSize: Int,
                      column: Option[String],
                      parameter: Option[Int],
                      reason: String): String = {
    val record = id match {
      case Some(value) => s"record '$value'"
      case None => s"one of $batchSize records"
    }
    val location = (column, parameter) match {
      case (Some(c), Some(p)) => s" at column \"$c\" (parameter $$$p)"
      case (Some(c), None) => s" at column \"$c\""
      case (None, Some(p)) => s" at parameter $$$p"
      case (None, None) => ""
    }
    s"Writing $record to store '$storeName' failed$location: $reason"
  }
}
