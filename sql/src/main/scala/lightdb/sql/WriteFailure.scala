package lightdb.sql

/**
 * What a driver's exception says about a failed write batch.
 *
 * @param entry the zero-based position in the batch of the statement that failed, when known
 * @param column the column the database refused, when it names one
 * @param parameter the one-based statement parameter the database refused, when it names one
 * @param reason the database's own words, without the statement
 */
case class WriteFailure(entry: Option[Int], column: Option[String], parameter: Option[Int], reason: String)
