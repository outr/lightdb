package spec

import fabric.rw.*
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.filter.*
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

/**
 * `Sort.BestMatch` ranks by the boosts of the clauses a row matched, whatever the field: a `should` or `must` clause adds its boost
 * (1 when none is given) to the score of every row it matches, and a group adds what its own clauses matched, times its boost, only
 * where the group as a whole matched. `filter` and `mustNot` clauses do not score. This is "more like this" over people and keywords:
 * one clause per shared id, each weighted by how much sharing it means.
 *
 * Scores are compared as differences from the lowest row's: a `must` clause adds the same amount to every row it keeps, and what
 * that amount is differs between engines (Lucene scores a string term by its rarity), while the differences do not.
 */
trait AbstractBoostedBestMatchSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  private lazy val specName: String = getClass.getSimpleName

  private val titles: List[Title] = List(
    Title("movie", cast = List(101, 102), keywords = List(201), _id = Id("sameLeads")),
    Title("movie", cast = List(101), _id = Id("sharesLead")),
    Title("movie", keywords = List(201, 202, 203), _id = Id("sharesKeywords")),
    Title("movie", keywords = List(203), _id = Id("sharesOneKeyword")),
    Title("movie", cast = List(150), keywords = List(250), _id = Id("unrelated")),
    Title("show", cast = List(101, 102), keywords = List(201, 202, 203), _id = Id("show"))
  )

  private def ranked(filter: Title.type => Filter[Title], limit: Int = 10): Task[List[(String, Double)]] =
    DB.titles.transaction(_.query.filter(filter).sort(Sort.BestMatch()).limit(limit).scored.search.flatMap(_.listWithScore))
      .map(_.map((t, s) => t._id.value -> s))

  /** Each row's score less the lowest row's, so a constant every row shares drops out. */
  private def relative(hits: List[(String, Double)]): List[(String, Double)] = {
    val floor = hits.map(_._2).min
    hits.map((id, s) => id -> math.round((s - floor) * 1000.0) / 1000.0)
  }

  private def likeSameLeads(t: Title.type) = t.builder
    .must(_.kind === "movie")
    .should(_.cast has 101, boost = Some(4.0))
    .should(_.cast has 102, boost = Some(3.0))
    .should(_.keywords has 201, boost = Some(1.0))
    .should(_.keywords has 202, boost = Some(1.0))
    .should(_.keywords has 203, boost = Some(0.5))

  specName should {
    "initialize the database" in {
      DB.init.succeed
    }
    "insert the titles" in {
      DB.titles.transaction(_.insert(titles)).succeed
    }
    "rank by the sum of the boosts each row matched, keeping only rows that matched one" in {
      ranked(likeSameLeads).map { hits =>
        relative(hits) should be(List("sameLeads" -> 7.5, "sharesLead" -> 3.5, "sharesKeywords" -> 2.0, "sharesOneKeyword" -> 0.0))
      }
    }
    "keep that order under a limit" in {
      ranked(likeSameLeads, limit = 2).map(_.map(_._1) should be(List("sameLeads", "sharesLead")))
    }
    "count a clause with no boost as 1" in {
      ranked(t => t.builder
        .must(_.kind === "movie")
        .should(_.keywords has 201)
        .should(_.keywords has 203, boost = Some(0.5))
      ).map { hits =>
        relative(hits) should be(List("sharesKeywords" -> 1.0, "sameLeads" -> 0.5, "sharesOneKeyword" -> 0.0))
      }
    }
    "accept a boost on a must clause" in {
      ranked(t => t.builder
        .must(_.kind === "movie", boost = Some(2.0))
        .should(_.cast has 101, boost = Some(4.0))
        .should(_.keywords has 203, boost = Some(0.5))
      ).map(_.map(_._1).toSet should be(Set("sameLeads", "sharesLead", "sharesKeywords", "sharesOneKeyword")))
    }
    "score a group by what it matched, times its boost, only where the group matched" in {
      ranked(t => t.builder
        .must(_.kind === "movie")
        .should(_.builder.must(_.cast has 101).must(_.cast has 102), boost = Some(2.0))
        .should(_.keywords has 203, boost = Some(0.5))
      ).map { hits =>
        // sharesLead holds 101 alone: the group needs both, so 101 scores nothing there and the row is not kept
        hits.head._1 should be("sameLeads")
        relative(hits).head._2 should be(3.5)
        hits.map(_._1).toSet should be(Set("sameLeads", "sharesKeywords", "sharesOneKeyword"))
      }
    }
    "truncate the database" in {
      DB.truncate().succeed
    }
    "dispose the database" in {
      DB.dispose.succeed
    }
  }

  def storeManager: CollectionManager

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager

    override def name: String = specName

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

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
