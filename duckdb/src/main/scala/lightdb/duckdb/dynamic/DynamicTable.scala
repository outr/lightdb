package lightdb.duckdb.dynamic

import fabric.*
import fabric.rw.*
import lightdb.{LightDB, Sort, SortDirection}
import lightdb.doc.{JsonDocument, JsonDocumentModel}
import lightdb.duckdb.{CsvImportControl, CsvImportOptions, CsvImportResult, DuckDBStore}
import lightdb.field.Field
import lightdb.filter.*
import lightdb.id.Id
import lightdb.sql.KeylessModel
import lightdb.upgrade.DatabaseUpgrade
import rapid.Task

import java.nio.file.{Files, Path}

/**
 * A table whose columns are declared at run time: a LightDB database of its own, made on demand (an ordinary class, initialised when
 * opened and disposed when let go, with no JVM shutdown hook), over DuckDB.
 *
 *  - Columns have kinds (text, integer, number, boolean); values are coerced as they are written, and a value that cannot be one names its column and row.
 *  - Rows are written, changed and removed by id (`insert`, `update`, `delete`), and read through LightDB's own queries (`query`, `count`): filters on any column,
 *    a search of every text column, sorts, pages.
 *  - A column can be added after (`addColumn`), existing rows holding no value for it.
 *  - A table loaded from a CSV (`fromCsv`) is the file's cache: read only, made again from the file.
 *
 * Column names are people's words (spaces and capitals allowed); the database holds them as `c0`, `c1`, ... so no name can clash with the store's own.
 */
final class DynamicTable private (dir: Path, meta: DynamicMeta, db: DynamicTable.Db) {
  def name: String = meta.name
  def columns: List[DynamicColumn] = meta.columns
  def readOnly: Boolean = meta.readOnly

  private val byName: Map[String, (DynamicColumn, Int)] = meta.columns.zipWithIndex.map((c, i) => c.name.toLowerCase -> (c, i)).toMap
  private def column(name: String): Either[String, (DynamicColumn, Int)] =
    byName.get(name.trim.toLowerCase).toRight(s"There is no column \"$name\"; the columns are: ${meta.columns.map(_.name).mkString(", ")}.")

  /** The column's field, for sorting and for reading (every column is nullable, so a row may leave one empty). */
  private def field(i: Int): Field[JsonDocument, Any] = db.model.fieldByName[Any](s"c$i")

  // ---- values ----

  /** `value` as column `c`'s kind, or why not. */
  private def coerce(c: DynamicColumn, value: Json): Either[String, Json] = (c.kind, value) match {
    case (_, Null)                           => Right(Null)
    case (ColumnKind.Text, s: Str)           => Right(s)
    case (ColumnKind.Text, n: NumInt)        => Right(str(n.value.toString))
    case (ColumnKind.Text, n: NumDec)        => Right(str(n.value.toString))
    case (ColumnKind.Text, b: Bool)          => Right(str(b.value.toString))
    case (ColumnKind.Integer, n: NumInt)     => Right(n)
    case (ColumnKind.Integer, n: NumDec) if n.value.isWhole =>
      if (n.value >= BigDecimal(Long.MinValue) && n.value <= BigDecimal(Long.MaxValue)) Right(num(n.value.toLong))
      else Left(s"${n.value.toBigInt} is too large for a whole-number column (column ${c.name}).")
    case (ColumnKind.Integer, s: Str)        => s.value.trim.toLongOption.map(l => num(l): Json).toRight(s"\"${s.value}\" is not a whole number (column ${c.name}).")
    case (ColumnKind.Number, n: NumInt)      => Right(num(n.value.toDouble))
    case (ColumnKind.Number, n: NumDec)      => Right(n)
    case (ColumnKind.Number, s: Str)         => s.value.trim.replace(",", "").toDoubleOption.map(d => num(d): Json).toRight(s"\"${s.value}\" is not a number (column ${c.name}).")
    case (ColumnKind.Boolean, b: Bool)       => Right(b)
    case (ColumnKind.Boolean, s: Str) =>
      s.value.trim.toLowerCase match {
        case "true" | "yes" | "y" | "1"  => Right(bool(true))
        case "false" | "no" | "n" | "0"  => Right(bool(false))
        case other                       => Left(s"\"$other\" is not true or false (column ${c.name}).")
      }
    case (kind, other) => Left(s"A ${kind.toString.toLowerCase} column (${c.name}) cannot hold $other.")
  }

  /** A row's values as the stored document holds them (`c0`, `c1`, ...), every column checked; the message says which row of an insert is wrong. */
  private def document(values: Map[String, Json], row: Option[Int]): Either[String, Json] = {
    val where = row.map(r => s"Row ${r + 1}: ").getOrElse("")
    values.toList.foldLeft[Either[String, List[(String, Json)]]](Right(Nil)) { (acc, kv) =>
      acc.flatMap(done => column(kv._1).left.map(where + _).flatMap { (c, i) => coerce(c, kv._2).left.map(where + _).map(v => (s"c$i" -> v) :: done) })
    }.map(fields => obj(fields*))
  }

  /** `document`, with a Null for each column the row leaves out (an insert writes every column). */
  private def whole(values: Map[String, Json], row: Option[Int]): Either[String, Json] =
    document(values, row).map(d => obj(meta.columns.indices.toList.map(i => s"c$i" -> d.get(s"c$i").getOrElse(Null))*))

  /** A stored value as its column's kind says (a boolean is held as 0 or 1). */
  private def shown(c: DynamicColumn, v: Json): Json = (c.kind, v) match {
    case (ColumnKind.Boolean, n: NumInt) => bool(n.value != 0L)
    case (ColumnKind.Boolean, n: NumDec) => bool(n.value != 0.0)
    case (ColumnKind.Number, n: NumInt)  => num(n.value.toDouble)
    case _                               => v
  }

  private def readRow(doc: Json, id: String): DynamicRow = DynamicRow(id, meta.columns.zipWithIndex.flatMap { (c, i) =>
    doc.get(s"c$i").filterNot(_.isNull).map(v => c.name -> shown(c, v))
  }.toMap)

  // ---- writing ----

  /** Adds `rows`; the ids they were given. Any row that is wrong refuses them all (nothing is written). */
  def insert(rows: Vector[Map[String, Json]]): Task[Either[String, Vector[String]]] =
    if (meta.readOnly) Task.pure(Left(s"${meta.name} is a copy of a file and cannot be changed."))
    else {
      val docs = rows.zipWithIndex.map((r, i) => whole(r, Some(i)))
      docs.collectFirst { case Left(why) => why } match {
        case Some(why) => Task.pure(Left(why))
        case None =>
          val made = docs.collect { case Right(d) => JsonDocument(json = d) }
          db.rows.transaction(tx => Task.sequence(made.toList.map(tx.insert(_).map(_._id.value)))).map(ids => Right(ids.toVector))
      }
    }

  /** Changes the values of row `id` named in `changes` (others as they were); whether the row was there. */
  def update(id: String, changes: Map[String, Json]): Task[Either[String, Boolean]] =
    if (meta.readOnly) Task.pure(Left(s"${meta.name} is a copy of a file and cannot be changed."))
    else document(changes, None) match {
      case Left(why) => Task.pure(Left(why))
      case Right(newer) =>
        db.rows.transaction { tx =>
          tx.get(Id[JsonDocument](id)).flatMap {
            case None => Task.pure(false)
            case Some(doc) =>
              val merged = obj((doc.json.asObj.value ++ newer.asObj.value).toList*)
              tx.upsert(doc.copy(json = merged)).map(_ => true)
          }
        }.map(Right(_))
    }

  /** Removes rows by id; how many were there. */
  def delete(ids: Vector[String]): Task[Either[String, Int]] =
    if (meta.readOnly) Task.pure(Left(s"${meta.name} is a copy of a file and cannot be changed."))
    else db.rows.transaction { tx =>
      Task.sequence(ids.toList.map(id => tx.get(Id[JsonDocument](id)).flatMap {
        case Some(_) => tx.delete(Id[JsonDocument](id)).map(_ => 1)
        case None    => Task.pure(0)
      })).map(_.sum)
    }.map(Right(_))

  /** Row `id`. */
  def get(id: String): Task[Option[DynamicRow]] =
    db.rows.transaction(_.query.filter(m => m._id === Id[JsonDocument](id)).json(m => m._id :: meta.columns.indices.toList.map(i => field(i))).toList)
      .map(_.headOption.map(j => readRow(j, j("_id").asString)))

  // ---- reading ----

  private def filterOf(f: ColumnFilter): Either[String, Filter[JsonDocument]] = column(f.column).flatMap { (c, i) =>
    val name = s"c$i"
    def bad(why: String): Either[String, Filter[JsonDocument]] = Left(s"Column ${c.name}: $why")
    def one(): Either[String, Json] = coerce(c, f.value)
    def plain(v: Json): Any = c.kind match {
      case ColumnKind.Text    => v.asString
      case ColumnKind.Integer => v.asLong
      case ColumnKind.Number  => v.asDouble
      case ColumnKind.Boolean => v.asBoolean
    }
    def many(): Either[String, List[Json]] = f.value match {
      case a: Arr => a.value.toList.foldLeft[Either[String, List[Json]]](Right(Nil))((acc, v) => acc.flatMap(l => coerce(c, v).map(_ :: l))).map(_.reverse)
      case _      => Left(s"Column ${c.name}: \"in\" takes a list of values.")
    }
    // a range, the bounds open or closed
    def range(v: Json, from: Boolean, inclusive: Boolean): Filter[JsonDocument] = c.kind match {
      case ColumnKind.Integer =>
        val l = v.asLong
        if (from) Filter.RangeLong[JsonDocument](name, Some(if (inclusive) l else l + 1), None) else Filter.RangeLong[JsonDocument](name, None, Some(if (inclusive) l else l - 1))
      case _ =>
        val d = v.asDouble
        if (from) Filter.RangeDouble[JsonDocument](name, Some(if (inclusive) d else Math.nextUp(d)), None) else Filter.RangeDouble[JsonDocument](name, None, Some(if (inclusive) d else Math.nextDown(d)))
    }
    f.op match {
      case FilterOp.Contains =>
        if (c.kind != ColumnKind.Text) bad("\"contains\" is for text columns.")
        else one().map(v => Filter.Regex[JsonDocument, String](name, "(?i)" + escape(v.asString)))
      case FilterOp.StartsWith =>
        if (c.kind != ColumnKind.Text) bad("\"starts with\" is for text columns.")
        else one().map(v => Filter.Regex[JsonDocument, String](name, "(?i)^" + escape(v.asString)))
      case FilterOp.In   => many().map(l => Filter.In[JsonDocument, Any](name, l.map(v => Some(plain(v)))))
      case FilterOp.Eq   => one().map(v => Filter.Equals[JsonDocument, Any](name, Some(plain(v))))
      case FilterOp.Ne   => one().map(v => Filter.NotEquals[JsonDocument, Any](name, Some(plain(v))))
      case op =>
        if (c.kind != ColumnKind.Integer && c.kind != ColumnKind.Number) bad("greater and less compare numbers; use a number column.")
        else one().map(v => op match {
          case FilterOp.Gt  => range(v, from = true, inclusive = false)
          case FilterOp.Gte => range(v, from = true, inclusive = true)
          case FilterOp.Lt  => range(v, from = false, inclusive = false)
          case _            => range(v, from = false, inclusive = true)
        })
    }
  }

  private def escape(word: String): String = word.flatMap(c => if ("\\.[]{}()*+?^$|".contains(c)) "\\" + c else c.toString)

  private def matching(q: DynamicQuery): Either[String, Option[Filter[JsonDocument]]] = {
    val filters = q.filters.foldLeft[Either[String, List[Filter[JsonDocument]]]](Right(Nil))((acc, f) => acc.flatMap(l => filterOf(f).map(_ :: l))).map(_.reverse)
    val texts = meta.columns.zipWithIndex.filter(_._1.kind == ColumnKind.Text).map(_._2)
    val words = q.search.split("\\s+").filter(_.nonEmpty).toList
    val search: List[Filter[JsonDocument]] =
      if (words.isEmpty || texts.isEmpty) Nil
      else List(words.map(w => texts.map(i => Filter.Regex[JsonDocument, String](s"c$i", "(?i)" + escape(w)): Filter[JsonDocument]).reduce(_ || _)).reduce(_ && _))
    filters.map(l => (l ++ search).reduceOption(_ && _))
  }

  private def order(q: DynamicQuery): Either[String, List[Sort]] =
    q.sort.foldLeft[Either[String, List[Sort]]](Right(Nil)) { (acc, s) =>
      acc.flatMap(l => column(s.column).map { (c, i) =>
        val d = if (s.descending) SortDirection.Descending else SortDirection.Ascending
        l :+ Sort.ByField(field(i), d)
      })
    }

  /** The rows `q` asks for, and how many match; or why the question is wrong. */
  def query(q: DynamicQuery): Task[Either[String, DynamicPage]] =
    (matching(q), order(q)) match {
      case (Left(why), _) => Task.pure(Left(why))
      case (_, Left(why)) => Task.pure(Left(why))
      case (Right(filter), Right(sorts)) =>
        val limit = math.max(1, math.min(q.limit, 5000))
        db.rows.transaction { tx =>
          val base = filter.fold(tx.query)(f => tx.query.filter(_ => f))
          // one search for the page and the count; the columns themselves are read, never the stored document (a table loaded from a CSV has none)
          val page = base.sort(sorts*).offset(math.max(0, q.offset)).limit(limit).countTotal(true)
            .json(m => m._id :: meta.columns.indices.toList.map(i => field(i)))
          page.search.flatMap(results => results.list.map { rows =>
            Right(DynamicPage(rows.toVector.map(j => readRow(j, j("_id").asString)), results.total.getOrElse(rows.size)))
          })
        }
    }

  /** How many rows `q` matches (its page ignored). */
  def count(q: DynamicQuery): Task[Either[String, Int]] =
    matching(q) match {
      case Left(why)     => Task.pure(Left(why))
      case Right(filter) => db.rows.transaction(tx => filter.fold(tx.query)(f => tx.query.filter(_ => f)).count).map(Right(_))
    }

  // ---- shape ----

  /** The table with column `c` added (rows hold no value for it); the table opens again as it is, and this one is let go. */
  def addColumn(c: DynamicColumn): Task[Either[String, DynamicTable]] =
    if (meta.readOnly) Task.pure(Left(s"${meta.name} is a copy of a file and cannot be changed."))
    else if (c.name.trim.isEmpty) Task.pure(Left("A column needs a name."))
    else if (byName.contains(c.name.trim.toLowerCase)) Task.pure(Left(s"There is already a column \"${c.name}\"."))
    else dispose.flatMap(_ => DynamicTable.write(dir, meta.copy(columns = meta.columns :+ c.copy(name = c.name.trim)))).flatMap(_ => DynamicTable.open(dir, Nil, meta.name)).map(Right(_))

  /** Closes the table's database (its files stay: it opens again from them). */
  def dispose: Task[Unit] = db.dispose
}

object DynamicTable {
  /** The LightDB database of one table. */
  private[dynamic] final class Db(dir: Path, columns: List[DynamicColumn], keyless: Boolean) extends LightDB {
    override type SM = DuckDBStore.type
    override val storeManager: DuckDBStore.type = DuckDBStore
    lazy val directory: Option[Path] = Some(dir.resolve("db"))
    override protected def disposeOnShutdown: Boolean = false
    override def upgrades: List[DatabaseUpgrade] = Nil
    val model: JsonDocumentModel = {
      val base = if keyless then new JsonDocumentModel("rows") with KeylessModel else JsonDocumentModel("rows")
      columns.zipWithIndex.foldLeft(base) { case (m, (c, i)) =>
        c.kind match {
          case ColumnKind.Text    => m.withField[Option[String]](s"c$i")
          case ColumnKind.Integer => m.withField[Option[Long]](s"c$i")
          case ColumnKind.Number  => m.withField[Option[Double]](s"c$i")
          case ColumnKind.Boolean => m.withField[Option[Boolean]](s"c$i")
        }
      }
    }
    val rows: DuckDBStore[JsonDocument, JsonDocumentModel] = store(model)()
  }

  private def metaFile(dir: Path): Path = dir.resolve("table.json")

  private[dynamic] def write(dir: Path, meta: DynamicMeta): Task[Unit] = Task {
    Files.createDirectories(dir)
    Files.writeString(metaFile(dir), meta.json.toString)
    ()
  }

  private def read(dir: Path): Option[DynamicMeta] =
    if (Files.exists(metaFile(dir))) scala.util.Try(fabric.io.JsonParser(Files.readString(metaFile(dir))).as[DynamicMeta]).toOption else None

  /** Whether a table has been made in `dir`. */
  def exists(dir: Path): Boolean = read(dir).nonEmpty

  /** The table in `dir`: opened as it was made, or made now with `columns` if there is none. Names are unique to a table, whatever their case. */
  def open(dir: Path, columns: List[DynamicColumn], name: String = "table"): Task[DynamicTable] = Task.defer {
    val meta = read(dir).getOrElse {
      val unique = columns.map(c => c.copy(name = c.name.trim)).filter(_.name.nonEmpty).groupBy(_.name.toLowerCase).values.map(_.head).toSet
      val ordered = columns.map(c => c.copy(name = c.name.trim)).filter(c => unique.contains(c))
      DynamicMeta(name, ordered)
    }
    if (read(dir).isEmpty && meta.columns.isEmpty) Task.error(new IllegalArgumentException("A table needs at least one column."))
    else write(dir, meta).flatMap { _ =>
      val db = new Db(dir, meta.columns, keyless = meta.readOnly)
      db.init.map(_ => new DynamicTable(dir, meta, db))
    }
  }

  /**
   * A table in `dir` made from the CSV `csv`: every column text, named by the file's headings (or Column 1, 2, ... without a header), loaded by DuckDB itself
   * (`DuckDBStore.importCsv`), read only. `names` are the columns, in the file's order.
   */
  def fromCsv(dir: Path, name: String, csv: Path, names: List[String], options: CsvImportOptions => CsvImportOptions = identity,
              control: CsvImportControl = new CsvImportControl): Task[(DynamicTable, CsvImportResult)] = Task.defer {
    val meta = DynamicMeta(name, names.map(n => DynamicColumn(n)), readOnly = true)
    write(dir, meta).flatMap { _ =>
      val db = new Db(dir, meta.columns, keyless = true)
      val base = CsvImportOptions(meta.columns.indices.toList.map(i => lightdb.duckdb.CsvColumn.byPlace(i + 1, s"c$i")), rowNumberField = None, replace = true)
      db.init.flatMap(_ => db.rows.transaction(tx => db.rows.importCsv(csv, options(base), control)(tx))).map(r => (new DynamicTable(dir, meta, db), r))
    }
  }
}
