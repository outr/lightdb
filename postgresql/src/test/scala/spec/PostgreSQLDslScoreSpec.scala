package spec

import fabric.rw.*
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.filter.*
import lightdb.id.Id
import lightdb.sql.SQLStoreTransaction
import lightdb.sql.dsl.SQLDsl
import lightdb.sql.dsl.SQLDsl.*
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

/**
 * A LightDB filter, and the score Sort.BestMatch ranks it by, rendered for a hand-built SQLDsl query: a caller whose query is built
 * in the DSL (joins, correlated subqueries, a union) can still pick and rank its rows with a standard boosted filter, without
 * writing the SQL itself. The embedded filter and score must keep exactly the rows, the order and the scores the store's own
 * query gives, including when the DSL query aliases the table, since the rendered SQL names columns unqualified.
 */
@EmbeddedTest
class PostgreSQLDslScoreSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with PostgreSQLAvailability {
  private val titles = List(
    Title("movie", cast = List(101, 102), keywords = List(201), _id = Id("sameLeads")),
    Title("movie", cast = List(101), _id = Id("sharesLead")),
    Title("movie", keywords = List(201, 202, 203), _id = Id("sharesKeywords")),
    Title("movie", keywords = List(203), _id = Id("sharesOneKeyword")),
    Title("movie", cast = List(150), keywords = List(250), _id = Id("unrelated")),
    Title("show", cast = List(101, 102), keywords = List(201, 202, 203), _id = Id("show"))
  )

  private def similar(t: Title.type): Filter[Title] = t.builder
    .must(_.kind === "movie")
    .should(_.cast has 101, boost = Some(4.0))
    .should(_.cast has 102, boost = Some(3.0))
    .should(_.keywords has 201, boost = Some(1.0))
    .should(_.keywords has 203, boost = Some(0.5))

  private def round(s: Double): Double = math.round(s * 1000.0) / 1000.0

  private def viaDsl(alias: Option[String]): Task[List[(String, Double)]] = DB.titles.transaction { tx =>
    val sqlTx = tx.asInstanceOf[SQLStoreTransaction[Title, Title.type]]
    val f = similar(Title)
    val t = alias.fold(table(DB.titles))(table(DB.titles).as(_))
    val q = select(colAs(col("_id"), "id"), valueAs(partValue(sqlTx.scoreSQL(f)), "score"))
      .from(t)
      .where(partExpr(sqlTx.filterSQL(f)))
      .orderBy(desc(col("score")))
      .toSQLQuery
    sqlTx.search[Row](q).flatMap(_.list)
  }.map(_.map(r => r.id -> round(r.score)))

  private def viaStore: Task[List[(String, Double)]] =
    DB.titles.transaction(_.query.filter(similar).sort(Sort.BestMatch()).scored.search.flatMap(_.listWithScore))
      .map(_.map((t, s) => t._id.value -> round(s)))

  "A boosted filter embedded in a SQLDsl query" should {
    "initialize the database" in {
      DB.init.flatMap(_ => DB.truncate()).succeed
    }
    "insert the titles" in {
      DB.titles.transaction(_.insert(titles)).succeed
    }
    "keep the rows, the order and the scores the store's own query gives" in {
      for {
        store <- viaStore
        dsl <- viaDsl(None)
      } yield {
        store.map(_._1) should be(List("sameLeads", "sharesLead", "sharesKeywords", "sharesOneKeyword"))
        dsl should be(store)
      }
    }
    "do the same when the DSL query aliases the table" in {
      for {
        store <- viaStore
        dsl <- viaDsl(Some("m"))
      } yield dsl should be(store)
    }
    "dispose the database" in {
      DB.dispose.succeed
    }
  }

  case class Row(id: String, score: Double)
  object Row {
    implicit val rw: RW[Row] = RW.gen
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = PostgreSQLTestSupport.storeManager
    override def name: String = "PostgreSQLDslScoreSpec"
    lazy val directory: Option[Path] = None
    val titles: Collection[Title, Title.type] = store(Title)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Title(kind: String,
                   cast: List[Int] = Nil,
                   keywords: List[Int] = Nil,
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Title] = Title.id()) extends RecordDocument[Title]

  object Title extends RecordDocumentModel[Title] with JsonConversion[Title] {
    override implicit val rw: RW[Title] = RW.gen
    val kind: I[String] = field.index("kind", (t: Title) => t.kind)
    val cast: I[List[Int]] = field.index("cast", (t: Title) => t.cast)
    val keywords: I[List[Int]] = field.index("keywords", (t: Title) => t.keywords)
  }
}
