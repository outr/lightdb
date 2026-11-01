package lightdb.duckdb

/** What an import did: the rows loaded, and how long DuckDB took. */
final case class CsvImportResult(rows: Long, millis: Long)
