package spec

import fabric.rw.*
import lightdb.{LightDB, Sort}
import lightdb.doc.{JsonDocument, JsonDocumentModel}
import lightdb.duckdb.{CsvColumn, CsvImportControl, CsvImportOptions, DuckDBStore}
import lightdb.filter.*
import lightdb.sql.KeylessModel
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.{Files, Path, Paths}

/** A CSV loaded by DuckDB itself into a store's table (`DuckDBStore.importCsv`), then read back through LightDB's own queries. */
@EmbeddedTest
class DuckDBCsvImportSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with BeforeAndAfterAll {
  private val root: Path = {
    val p = Paths.get("db/DuckDBCsvImportSpec")
    if Files.exists(p) then Files.walk(p).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete(_))
    Files.createDirectories(p)
    p
  }
  private var made = 0

  private def csv(text: String): Path = { made += 1; val f = root.resolve(s"file$made.csv"); Files.writeString(f, text); f }

  private val LargeRows = 3_000_000

  /** A generated file of about 300 MB (id, name, amount, note), kept under the user's cache directory and deleted after the spec: every
    * tenth amount is not a number. */
  private lazy val large: Path = {
    val dir = Paths.get(System.getProperty("user.home"), ".cache", "lightdb-test", "DuckDBCsvImportSpec")
    Files.createDirectories(dir)
    val f = dir.resolve("large.csv")
    val w = Files.newBufferedWriter(f)
    try {
      w.write("id,name,amount,note\n")
      val note = "a note long enough to make each row about a hundred bytes, quoted, with a comma"
      var i = 1
      while i <= LargeRows do {
        val amount = if i % 10 == 0 then "n/a" else i.toString
        w.write(s"$i,name$i,$amount,\"$note\"\n")
        i += 1
      }
    } finally w.close()
    f
  }

  private val largeOptions = CsvImportOptions(
    List(CsvColumn.byHeading("id", "c_id"), CsvColumn.byHeading("name", "c_name"), CsvColumn.byHeading("amount", "c_code"),
      CsvColumn.byHeading("amount", "c_num", Some("DOUBLE")), CsvColumn.byHeading("note", "c_note")),
    rowNumberField = Some("rn")
  )

  override protected def afterAll(): Unit = {
    Files.deleteIfExists(Paths.get(System.getProperty("user.home"), ".cache", "lightdb-test", "DuckDBCsvImportSpec", "large.csv"))
    super.afterAll()
  }

  /** A database for one CSV, made on demand: an ordinary LightDB class, initialised, used and disposed, with no JVM shutdown hook. */
  private class CsvDb(keyless: Boolean) extends LightDB {
    made += 1
    override type SM = DuckDBStore.type
    override val storeManager: DuckDBStore.type = DuckDBStore
    lazy val directory: Option[Path] = Some(root.resolve(s"db$made"))
    override protected def disposeOnShutdown: Boolean = false
    override def upgrades: List[DatabaseUpgrade] = Nil
    private def plain: JsonDocumentModel = if keyless then new JsonDocumentModel("csv") with KeylessModel else JsonDocumentModel("csv")
    val model: JsonDocumentModel = List("c_id", "c_name", "c_note", "c_code").foldLeft(plain.withField[Long]("rn").withField[Double]("c_num"))((m, c) => m.withField[String](c))
    val rows: DuckDBStore[JsonDocument, JsonDocumentModel] = store(model)()
  }

  private def withDb[A](keyless: Boolean = true)(f: CsvDb => Task[A]): Task[A] = {
    val db = new CsvDb(keyless)
    db.init.flatMap(_ => f(db)).guarantee(db.dispose)
  }

  private val byHeading = List(CsvColumn.byHeading("id", "c_id"), CsvColumn.byHeading("name", "c_name"), CsvColumn.byHeading("note", "c_note"), CsvColumn.byHeading("code", "c_code"))

  private val ragged = "id,name,note\n1,a,x\n2,b\n3,c,z,extra\n4,d,w\n"

  private val tricky =
    "id,name,note,code\n" +
    "1,Ada,\"said \"\"hi\"\"\",007\n" +
    "2,Alan,\"line one\nline two\",12\n" +
    "3,Grace,tag #1,3.50\n"

  "DuckDBStore.importCsv" should {
    "load quoted fields, doubled quotes, a line break in a quote and a # in data, as text, numbered from 1" in {
      withDb() { db =>
        val rn = db.model.fieldByName[Long]("rn")
        val note = db.model.fieldByName[String]("c_note")
        val code = db.model.fieldByName[String]("c_code")
        for {
          result <- db.rows.transaction(tx => db.rows.importCsv(csv(tricky), CsvImportOptions(byHeading, rowNumberField = Some("rn")))(tx))
          count  <- db.rows.transaction(_.count)
          notes  <- db.rows.transaction(_.query.sort(Sort.ByField(rn)).value(_ => note).toList)
          codes  <- db.rows.transaction(_.query.sort(Sort.ByField(rn)).value(_ => code).toList)
          rns    <- db.rows.transaction(_.query.sort(Sort.ByField(rn)).value(_ => rn).toList)
        } yield {
          result.rows should be(3L)
          count should be(3)
          notes should be(List("said \"hi\"", "line one\nline two", "tag #1"))
          codes should be(List("007", "12", "3.50"))   // the text the file holds: a leading zero and a trailing zero stay
          rns should be(List(1L, 2L, 3L))
        }
      }
    }
    "give a column a numeric twin that is NULL where the text is not a number, and sort by it" in {
      withDb() { db =>
        val file = csv("id,name,note,code\n1,a,x,10\n2,b,y,9\n3,c,z,n/a\n4,d,w,100\n")
        val options = CsvImportOptions(byHeading ++ List(CsvColumn.byHeading("code", "c_num", Some("DOUBLE"))), rowNumberField = Some("rn"))
        val local = db
        for {
          _     <- db.rows.transaction(tx => db.rows.importCsv(file, options)(tx))
          text  <- db.rows.transaction(_.query.sort(Sort.ByField(db.model.fieldByName[String]("c_code")).descending).value(_ => db.model.fieldByName[String]("c_name")).toList)
          num   <- db.rows.transaction(_.query.filter(_ => db.model.fieldByName[Double]("c_num") >= 0.0).sort(Sort.ByField(db.model.fieldByName[Double]("c_num")).descending).value(_ => db.model.fieldByName[String]("c_name")).toList)
        } yield {
          text should be(List("c", "b", "d", "a"))   // as text: "n/a" first, then "9" before "100" and "10"
          num should be(List("d", "a", "b"))         // by value: 100, 10, 9; the one that is not a number is out
        }
      }
    }
    "pick columns by place from a file with no header and note lines to skip" in {
      withDb() { db =>
        val name = db.model.fieldByName[String]("c_name")
        val file = csv("# made by a tool\n# for a test\n1,a\n2,b\n3,c\n")
        val options = CsvImportOptions(List(CsvColumn.byPlace(1, "c_id"), CsvColumn.byPlace(2, "c_name")), header = false, skipLines = 2, rowNumberField = Some("rn"))
        for {
          result <- db.rows.transaction(tx => db.rows.importCsv(file, options)(tx))
          names  <- db.rows.transaction(_.query.sort(Sort.ByField(db.model.fieldByName[Long]("rn"))).value(_ => name).toList)
        } yield {
          result.rows should be(3L)
          names should be(List("a", "b", "c"))
        }
      }
    }
    "replace what the table holds only when asked" in {
      withDb() { db =>
        val file = csv(tricky)
        def load(replace: Boolean) = db.rows.transaction(tx => db.rows.importCsv(file, CsvImportOptions(byHeading, rowNumberField = Some("rn"), replace = replace))(tx))
        for {
          _     <- load(replace = false)
          _     <- load(replace = false)
          twice <- db.rows.transaction(_.count)
          _     <- load(replace = true)
          once  <- db.rows.transaction(_.count)
        } yield { twice should be(6); once should be(3) }
      }
    }
    "name the columns the file has when one asked for is not there, and change nothing" in {
      withDb() { db =>
        val options = CsvImportOptions(List(CsvColumn.byHeading("nope", "c_id")), rowNumberField = Some("rn"))
        for {
          failure <- db.rows.transaction(tx => db.rows.importCsv(csv(tricky), options)(tx)).attempt
          count   <- db.rows.transaction(_.count)
        } yield {
          failure.isFailure should be(true)
          failure.failed.get.getMessage should (include("nope") and include("id, name, note, code"))
          count should be(0)
        }
      }
    }
    "filter, sort and count what it loaded through LightDB's own queries" in {
      withDb() { db =>
        val name = db.model.fieldByName[String]("c_name")
        val rn = db.model.fieldByName[Long]("rn")
        for {
          _     <- db.rows.transaction(tx => db.rows.importCsv(csv(tricky), CsvImportOptions(byHeading, rowNumberField = Some("rn")))(tx))
          grace <- db.rows.transaction(_.query.filter(_ => name === "Grace").value(_ => rn).toList)
          a     <- db.rows.transaction(_.query.filter(_ => name.startsWith("A")).count)
          last  <- db.rows.transaction(_.query.sort(Sort.ByField(name).descending).limit(1).value(_ => name).toList)
        } yield { grace should be(List(3L)); a should be(2); last should be(List("Grace")) }
      }
    }
    "leave a keyless model with no primary key, and any other with one" in {
      def keys(db: CsvDb): Task[Long] = db.rows.transaction { tx =>
        Task {
          val s = tx.state.openConnection().createStatement()
          try { val r = s.executeQuery(s"SELECT count(*) FROM duckdb_constraints() WHERE table_name = 'csv' AND constraint_type = 'PRIMARY KEY'"); r.next(); r.getLong(1) } finally s.close()
        }
      }
      for {
        keyless <- withDb(keyless = true)(keys)
        keyed   <- withDb(keyless = false)(keys)
      } yield { keyless should be(0L); keyed should be(1L) }
    }
    "skip a preamble before the header and read the columns by their headings" in {
      withDb() { db =>
        val name = db.model.fieldByName[String]("c_name")
        val file = csv("Exported by a tool\nfor a test\nid,name\n1,a\n2,b\n")
        val options = CsvImportOptions(List(CsvColumn.byHeading("id", "c_id"), CsvColumn.byHeading("name", "c_name")), skipLines = 2, rowNumberField = Some("rn"))
        for {
          result <- db.rows.transaction(tx => db.rows.importCsv(file, options)(tx))
          names <- db.rows.transaction(_.query.sort(Sort.ByField(db.model.fieldByName[Long]("rn"))).value(_ => name).toList)
        } yield {
          result.rows should be(2L)
          names should be(List("a", "b"))
        }
      }
    }
    "pad a short row with NULLs and take a long row's extra value as a column of its own" in {
      withDb() { db =>
        val name = db.model.fieldByName[String]("c_name")
        val rn = db.model.fieldByName[Long]("rn")
        val options = CsvImportOptions(
          List(CsvColumn.byPlace(1, "c_id"), CsvColumn.byPlace(2, "c_name"), CsvColumn.byPlace(3, "c_note"), CsvColumn.byPlace(4, "c_code")),
          rowNumberField = Some("rn"), nullPadding = true
        )
        for {
          result <- db.rows.transaction(tx => db.rows.importCsv(csv(ragged), options)(tx))
          names <- db.rows.transaction(_.query.sort(Sort.ByField(rn)).value(_ => name).toList)
          // in SQL: a NULL read through a String field (not an Option) comes back as the text "null"
          nulls <- db.rows.transaction(tx => Task {
            val s = tx.state.openConnection().createStatement()
            try {
              val r = s.executeQuery("SELECT string_agg(coalesce(c_note, '-'), ',' ORDER BY rn), string_agg(coalesce(c_code, '-'), ',' ORDER BY rn) FROM csv")
              r.next()
              (r.getString(1), r.getString(2))
            } finally s.close()
          })
        } yield {
          result.rows should be(4L)
          names should be(List("a", "b", "c", "d"))
          nulls should be(("x,-,z,w", "-,-,extra,-"))
        }
      }
    }
    "fail on a ragged row without padding, changing nothing, and skip the ragged rows when asked to" in {
      withDb() { db =>
        val name = db.model.fieldByName[String]("c_name")
        val rn = db.model.fieldByName[Long]("rn")
        val file = csv(ragged)
        def options(ignoreErrors: Boolean) = CsvImportOptions(
          List(CsvColumn.byPlace(1, "c_id"), CsvColumn.byPlace(2, "c_name"), CsvColumn.byPlace(3, "c_note")),
          rowNumberField = Some("rn"), ignoreErrors = ignoreErrors
        )
        for {
          strict <- db.rows.transaction(tx => db.rows.importCsv(file, options(ignoreErrors = false))(tx)).attempt
          none <- db.rows.transaction(_.count)
          lenient <- db.rows.transaction(tx => db.rows.importCsv(file, options(ignoreErrors = true))(tx))
          names <- db.rows.transaction(_.query.sort(Sort.ByField(rn)).value(_ => name).toList)
        } yield {
          strict.isFailure should be(true)
          none should be(0)
          lenient.rows should be(2L)
          names should be(List("a", "d"))
        }
      }
    }
    "fail on an unterminated quote even when skipping bad rows, changing nothing" in {
      withDb() { db =>
        val file = csv("id,name,note\n1,a,x\n2,\"b,y\n3,c,z\n")
        val options = CsvImportOptions(byHeading.take(3), rowNumberField = Some("rn"), nullPadding = true, ignoreErrors = true)
        for {
          outcome <- db.rows.transaction(tx => db.rows.importCsv(file, options)(tx)).attempt
          count <- db.rows.transaction(_.count)
        } yield {
          outcome.isFailure should be(true)
          count should be(0)
        }
      }
    }
    "stop an import that is running when its control is cancelled, leaving the table as it was" in {
      withDb() { db =>
        val control = new CsvImportControl
        val canceller = new Thread(() => { Thread.sleep(150); control.cancel() })
        canceller.setDaemon(true)
        for {
          _ <- Task(large)
          _ <- Task(canceller.start())
          outcome <- db.rows.transaction(tx => db.rows.importCsv(large, largeOptions, control)(tx)).attempt
          count <- db.rows.transaction(_.count)
        } yield {
          control.isCancelled should be(true)
          outcome.failed.get shouldBe a[java.util.concurrent.CancellationException]
          count should be(0)
        }
      }
    }
    "load a file of a few hundred megabytes with the JVM's memory flat and DuckDB's held to a limit, numbered in the file's order" in {
      withDb() { db =>
        val rn = db.model.fieldByName[Long]("rn")
        val id = db.model.fieldByName[String]("c_id")
        val amount = db.model.fieldByName[Double]("c_num")
        val heap = java.lang.management.ManagementFactory.getMemoryMXBean
        def used: Long = heap.getHeapMemoryUsage.getUsed
        @volatile var peak = 0L
        @volatile var sampling = true
        val sampler = new Thread(() => while sampling do { peak = math.max(peak, used); Thread.sleep(10) })
        sampler.setDaemon(true)
        for {
          _ <- db.rows.transaction(tx => Task {
            val s = tx.state.openConnection().createStatement()
            try s.execute("SET memory_limit = '512MB'") finally s.close()
          })
          baseline <- Task { System.gc(); used }
          _ <- Task(sampler.start())
          result <- db.rows.transaction(tx => db.rows.importCsv(large, largeOptions)(tx)).guarantee(Task { sampling = false })
          count <- db.rows.transaction(_.count)
          // the row number is the line's own id: no row was numbered out of the file's order
          misnumbered <- db.rows.transaction(tx => Task {
            val s = tx.state.openConnection().createStatement()
            try {
              val r = s.executeQuery(s"SELECT count(*) FROM csv WHERE CAST(c_id AS BIGINT) <> rn")
              r.next()
              r.getLong(1)
            } finally s.close()
          })
          top <- db.rows.transaction(_.query.filter(_ => amount >= 0.0).sort(Sort.ByField(amount).descending).limit(3).value(_ => id).toList)
          numeric <- db.rows.transaction(_.query.filter(_ => amount >= 0.0).count)
          page <- db.rows.transaction(_.query.sort(Sort.ByField(rn)).offset(1_000_000).limit(2).value(_ => rn).toList)
        } yield {
          Files.size(large) should be > 250_000_000L
          result.rows should be(LargeRows.toLong)
          count should be(LargeRows)
          misnumbered should be(0L)
          // every tenth amount is not a number, so it is NULL in the numeric twin
          numeric should be(LargeRows - LargeRows / 10)
          top should be(List(LargeRows - 1, LargeRows - 2, LargeRows - 3).map(_.toString))
          page should be(List(1_000_001L, 1_000_002L))
          (peak - baseline) should be < (128L * 1024 * 1024)
        }
      }
    }
  }
}
