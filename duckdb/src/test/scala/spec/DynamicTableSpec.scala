package spec

import fabric.*
import lightdb.duckdb.dynamic.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.{Files, Path, Paths}

/** A table whose columns are made at run time: typed values, rows written and changed and removed by id, filters, search, sorts and pages through LightDB's
  * queries, a column added after, reopening from disk, and a table loaded from a CSV (read only). */
@EmbeddedTest
class DynamicTableSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  private val root: Path = {
    val p = Paths.get("db/DynamicTableSpec")
    if (Files.exists(p)) Files.walk(p).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete(_))
    Files.createDirectories(p)
    p
  }
  private var made = 0
  private def dir(): Path = { made += 1; root.resolve(s"t$made") }

  private val columns = List(DynamicColumn("Name"), DynamicColumn("Age", ColumnKind.Integer), DynamicColumn("Score", ColumnKind.Number), DynamicColumn("Active", ColumnKind.Boolean))
  private def row(name: String, age: Int, score: Double, active: Boolean): Map[String, Json] =
    Map("Name" -> str(name), "Age" -> num(age), "Score" -> num(score), "Active" -> bool(active))
  private val people = Vector(row("Ada Lovelace", 36, 9.5, true), row("Alan Turing", 41, 9.9, true), row("Grace Hopper", 85, 9.7, false), row("Linus", 54, 8.0, true), row("Ken", 81, 7.25, false))

  private def withTable[A](f: DynamicTable => Task[A]): Task[A] =
    DynamicTable.open(dir(), columns, "people").flatMap(t => t.insert(people).flatMap(_ => f(t)).guarantee(t.dispose))
  private def names(p: Either[String, DynamicPage]): List[String] = p.toOption.get.rows.map(_.values("Name").asString).toList

  "A dynamic table" should {
    "hold typed rows and read them back by the column's own name" in {
      withTable { t =>
        t.query(DynamicQuery(sort = List(SortBy("Age")))).map { p =>
          p.toOption.get.total shouldBe 5
          p.toOption.get.rows.head.values("Name") shouldBe str("Ada Lovelace")
          p.toOption.get.rows.head.values("Age") shouldBe num(36)
          p.toOption.get.rows.head.values("Active") shouldBe bool(true)
          names(p) shouldBe List("Ada Lovelace", "Alan Turing", "Linus", "Ken", "Grace Hopper")
        }
      }
    }
    "coerce what can be (a numeral in a text, a word for a boolean) and refuse what cannot, naming the row and column, writing nothing" in {
      withTable { t =>
        for {
          ok   <- t.insert(Vector(Map("Name" -> num(7), "Age" -> str("30"), "Score" -> str("1,234.5"), "Active" -> str("yes"))))
          bad  <- t.insert(Vector(row("Fine", 20, 1.0, true), Map("Name" -> str("Bad"), "Age" -> str("old"))))
          none <- t.insert(Vector(Map("Nope" -> str("x"))))
          n    <- t.count(DynamicQuery())
          seven <- t.query(DynamicQuery(filters = List(ColumnFilter("Name", FilterOp.Eq, str("7")))))
        } yield {
          ok.isRight shouldBe true
          bad.left.toOption.get should (include("Row 2") and include("Age") and include("\"old\""))
          none.left.toOption.get should (include("no column \"Nope\"") and include("Name, Age, Score, Active"))
          n shouldBe Right(6)    // the five and the coerced one; "Fine" was not written with the bad row after it
          seven.toOption.get.rows.head.values("Score") shouldBe num(1234.5)
          seven.toOption.get.rows.head.values("Active") shouldBe bool(true)
        }
      }
    }
    "refuse a whole number too large for an integer column rather than wrap it" in {
      withTable { t =>
        for {
          big <- t.insert(Vector(Map("Name" -> str("Big"), "Age" -> fabric.NumDec(BigDecimal("9223372036854775808")))))
          max <- t.insert(Vector(Map("Name" -> str("Max"), "Age" -> fabric.NumInt(Long.MaxValue))))
        } yield {
          big.left.toOption.get should (include("9223372036854775808") and include("Age"))
          max.isRight shouldBe true
        }
      }
    }
    "filter by each comparison, on the column's kind" in {
      withTable { t =>
        def q(c: String, op: FilterOp, v: Json) = t.query(DynamicQuery(filters = List(ColumnFilter(c, op, v)), sort = List(SortBy("Age")))).map(names)
        for {
          eq   <- q("Name", FilterOp.Eq, str("Linus"))
          ne   <- q("Active", FilterOp.Ne, bool(true))
          gt   <- q("Age", FilterOp.Gt, num(54))
          gte  <- q("Age", FilterOp.Gte, num(54))
          lt   <- q("Score", FilterOp.Lt, num(8.0))
          lte  <- q("Score", FilterOp.Lte, num(8.0))
          has  <- q("Name", FilterOp.Contains, str("RACE"))
          st   <- q("Name", FilterOp.StartsWith, str("al"))
          in   <- q("Name", FilterOp.In, arr(str("Ken"), str("Linus"), str("Nobody")))
          wrong <- t.query(DynamicQuery(filters = List(ColumnFilter("Name", FilterOp.Gt, num(1)))))
          unk  <- t.query(DynamicQuery(filters = List(ColumnFilter("Zip", FilterOp.Eq, str("1")))))
        } yield {
          eq shouldBe List("Linus"); ne shouldBe List("Ken", "Grace Hopper")
          gt shouldBe List("Ken", "Grace Hopper"); gte shouldBe List("Linus", "Ken", "Grace Hopper")
          lt shouldBe List("Ken"); lte shouldBe List("Linus", "Ken")
          has shouldBe List("Grace Hopper"); st shouldBe List("Alan Turing"); in shouldBe List("Linus", "Ken")
          wrong.isLeft shouldBe true; unk.left.toOption.get should include("no column \"Zip\"")
        }
      }
    }
    "search the text columns for every word, whatever the case, and page and sort what matches" in {
      withTable { t =>
        for {
          a    <- t.query(DynamicQuery(search = "AL", sort = List(SortBy("Name"))))
          al2  <- t.query(DynamicQuery(search = "LOVE", sort = List(SortBy("Name"))))
          two  <- t.query(DynamicQuery(search = "lace ada"))
          none <- t.query(DynamicQuery(search = "zzz"))
          page <- t.query(DynamicQuery(sort = List(SortBy("Score", descending = true)), offset = 1, limit = 2))
          withFilter <- t.query(DynamicQuery(filters = List(ColumnFilter("Active", FilterOp.Eq, bool(true))), search = "a", sort = List(SortBy("Age", descending = true))))
        } yield {
          names(a) shouldBe List("Alan Turing")   // "al" is in Alan only (not in Ada Lovelace)
          names(al2) shouldBe List("Ada Lovelace")
          names(two) shouldBe List("Ada Lovelace")
          none.toOption.get.total shouldBe 0
          names(page) shouldBe List("Grace Hopper", "Ada Lovelace")   // by value: 9.9, 9.7, 9.5, 8.0, 7.25
          page.toOption.get.total shouldBe 5
          names(withFilter) shouldBe List("Alan Turing", "Ada Lovelace")
        }
      }
    }
    "change a row's values by id, leave the others, and remove rows" in {
      withTable { t =>
        for {
          found <- t.query(DynamicQuery(filters = List(ColumnFilter("Name", FilterOp.Eq, str("Ken")))))
          id     = found.toOption.get.rows.head.id
          upd   <- t.update(id, Map("Age" -> num(82), "Active" -> bool(true)))
          again <- t.get(id)
          badUpd <- t.update(id, Map("Age" -> str("old")))
          missing <- t.update("no-such-id", Map("Age" -> num(1)))
          gone  <- t.delete(Vector(id, "no-such-id"))
          after <- t.count(DynamicQuery())
        } yield {
          upd shouldBe Right(true)
          again.get.values("Age") shouldBe num(82); again.get.values("Name") shouldBe str("Ken"); again.get.values("Active") shouldBe bool(true); again.get.values("Score") shouldBe num(7.25)
          badUpd.isLeft shouldBe true
          missing shouldBe Right(false)
          gone shouldBe Right(1)
          after shouldBe Right(4)
        }
      }
    }
    "gain a column, the rows holding nothing for it, and open again from disk as it was" in {
      val d = dir()
      for {
        t     <- DynamicTable.open(d, columns, "people")
        _     <- t.insert(people)
        t2    <- t.addColumn(DynamicColumn("City")).map(_.toOption.get)
        dup   <- t2.addColumn(DynamicColumn("city"))
        _     <- t2.insert(Vector(Map("Name" -> str("Zed"), "City" -> str("Oslo"))))
        found <- t2.query(DynamicQuery(filters = List(ColumnFilter("City", FilterOp.Eq, str("Oslo")))))
        old   <- t2.query(DynamicQuery(filters = List(ColumnFilter("Name", FilterOp.Eq, str("Linus")))))
        _     <- t2.dispose
        t3    <- DynamicTable.open(d, Nil)
        total <- t3.count(DynamicQuery())
        _     <- t3.dispose
      } yield {
        dup.isLeft shouldBe true
        names(found) shouldBe List("Zed")
        old.toOption.get.rows.head.values.get("City") shouldBe None
        t3.columns.map(_.name) shouldBe List("Name", "Age", "Score", "Active", "City")
        total shouldBe Right(6)
      }
    }
    "filter, sort and page on a column added after, across reopening" in {
      val d = dir()
      for {
        t <- DynamicTable.open(d, columns, "people")
        _ <- t.insert(people)
        t2 <- t.addColumn(DynamicColumn("Rank", ColumnKind.Integer)).map(_.toOption.get)
        _ <- t2.insert((1 to 30).toVector.map(i => Map("Name" -> str(s"P$i"), "Rank" -> num(i))))
        _ <- t2.dispose
        t3 <- DynamicTable.open(d, Nil)
        page <- t3.query(DynamicQuery(filters = List(ColumnFilter("Rank", FilterOp.Gt, num(10))), sort = List(SortBy("Rank", descending = true)), offset = 5, limit = 4))
        last <- t3.query(DynamicQuery(filters = List(ColumnFilter("Rank", FilterOp.Gte, num(1))), sort = List(SortBy("Rank")), offset = 28, limit = 10))
        unranked <- t3.count(DynamicQuery(filters = List(ColumnFilter("Rank", FilterOp.In, arr(num(1), num(2))))))
        _ <- t3.dispose
      } yield {
        page.toOption.get.rows.map(_.values("Rank")) shouldBe Vector(num(25), num(24), num(23), num(22))
        page.toOption.get.total shouldBe 20
        names(last) shouldBe List("P29", "P30")
        last.toOption.get.total shouldBe 30
        unranked shouldBe Right(2)
      }
    }
    "load a CSV as a read-only table of text, and be queried the same way" in {
      val csv = root.resolve("people.csv")
      Files.writeString(csv, "name,amount,note\nAda,9.5,\"likes \"\"math\"\"\"\nAlan,9.9,codes\nGrace,9.7,#compilers\n")
      for {
        made  <- DynamicTable.fromCsv(dir(), "people-file", csv, List("name", "amount", "note"))
        (t, result) = made
        all   <- t.query(DynamicQuery(sort = List(SortBy("name"))))
        found <- t.query(DynamicQuery(search = "MATH"))
        ins   <- t.insert(Vector(Map("name" -> str("x"))))
        _     <- t.dispose
      } yield {
        result.rows shouldBe 3L
        all.toOption.get.rows.map(r => (r.values("name").asString, r.values("note").asString)) shouldBe Vector(("Ada", "likes \"math\""), ("Alan", "codes"), ("Grace", "#compilers"))
        names(found.map(p => p.copy(rows = p.rows.map(r => r.copy(values = r.values.map((k, v) => if (k == "name") "Name" -> v else k -> v)))))) shouldBe List("Ada")
        ins.left.toOption.get should include("copy of a file")
      }
    }
  }
}
