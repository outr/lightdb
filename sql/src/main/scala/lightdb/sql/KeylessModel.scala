package lightdb.sql

/**
 * Marks a model whose SQL table has an `_id` column but no primary key on it. The key is an index over every row, built as the
 * rows are written and held with them: for a table that is loaded once in bulk (a CSV imported by `DuckDBStore.importCsv`) and
 * read after, it is cost and no use (10 million rows: the load about 1.8 times slower and the file about 2.4 times larger).
 *
 * What a key gives is gone with it: an upsert (`ON CONFLICT(_id)`) and a lookup by id (a scan) have nothing to use, and two rows
 * may hold the same id. A keyless table is for loading and querying, never for writing document by document.
 */
trait KeylessModel
