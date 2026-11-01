package lightdb.duckdb

import java.nio.file.Path

private[duckdb] object CsvImport {
  /** A string as a SQL literal. */
  def literal(value: String): String = "'" + value.replace("'", "''") + "'"

  /** A SQL type name as given, only if it is one word of letters, digits and underscores (it goes into the statement as it is). */
  def typeName(name: String): String =
    if name.matches("[A-Za-z][A-Za-z0-9_]*(\\([0-9, ]+\\))?") then name else throw new IllegalArgumentException(s"Not a type name: $name")

  def literal(path: Path): String = literal(path.toAbsolutePath.toString)

  /** The statement's options for reading `path`: always the dialect, text columns unless asked. */
  def readCsv(path: Path, o: CsvImportOptions): String = {
    val parts = List(
      s"header=${o.header}",
      s"delim=${literal(o.delimiter.toString)}",
      s"quote=${literal(o.quote.toString)}",
      s"escape=${literal(o.escape.toString)}",
      s"all_varchar=${o.allVarchar}",
      s"null_padding=${o.nullPadding}",
      s"ignore_errors=${o.ignoreErrors}"
    ) ++ Option.when(o.skipLines > 0)(s"skip=${o.skipLines}")
    s"read_csv(${literal(path)}, ${parts.mkString(", ")})"
  }
}
