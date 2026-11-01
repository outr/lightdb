package lightdb.duckdb

/**
 * What a CSV file is read as when `DuckDBStore.importCsv` loads it.
 *
 * The file is read by DuckDB itself, natively and in parallel (`read_csv`), straight into the store's table: no row passes through
 * the JVM. Every column is read as text (`allVarchar`, the default), so a value is stored exactly as the file holds it (a leading
 * zero, a long number, a date in any form); a store field that is numeric is cast by DuckDB as the rows are inserted.
 *
 * The dialect is always given, never sniffed: DuckDB's own detection missed the quote of a file whose quoted fields were rare.
 * A `#` inside a line is data. (DuckDB's `comment` option would cut the line there, so it is not offered: skip leading note
 * lines with `skipLines`.)
 *
 * @param columns        which CSV column goes to which field of the store; a field not named here is left NULL
 * @param rowNumberField a field (a numeric one) that receives each row's number, 1 for the first row after any header
 * @param idFromRowNumber the `_id` of each row is its number (as text); else a unique id
 * @param replace        delete the rows the table holds first, in the same transaction
 * @param nullPadding    a row short of columns has NULLs for the rest; the file is read as wide as its widest row, so a row's extra
 *                       values are columns past the last heading (read them by place) rather than an error
 * @param ignoreErrors   a row that cannot be read as the file's shape (too few or too many values without `nullPadding`, a value
 *                       that cannot be cast) is skipped rather than failing the import. A quote left open fails the import whatever
 *                       this says: DuckDB cannot tell where the file's rows are.
 */
final case class CsvImportOptions(columns: List[CsvColumn],
                                  delimiter: Char = ',',
                                  quote: Char = '"',
                                  escape: Char = '"',
                                  header: Boolean = true,
                                  skipLines: Int = 0,
                                  rowNumberField: Option[String] = None,
                                  idFromRowNumber: Boolean = true,
                                  replace: Boolean = false,
                                  allVarchar: Boolean = true,
                                  ignoreErrors: Boolean = false,
                                  nullPadding: Boolean = false)
