package spec

import fabric.rw._
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.facet.{FacetConfig, FacetValue}
import lightdb.filter._
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Id, LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

abstract class AbstractFacetSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected lazy val specName: String = getClass.getSimpleName

  protected var db: DB = new DB

  protected val one = Entry("One", List("Bob", "James"), List("support@one.com", "support@two.com"), PublishDate(2010, 10, 15))
  protected val two = Entry("Two", List("Lisa"), List("support@one.com"), PublishDate(2010, 10, 20))
  protected val three = Entry("Three", List("Lisa"), List("support@two.com"), PublishDate(2012, 1, 1))
  protected val four = Entry("Four", List("Susan"), List("support@three.com"), PublishDate(2012, 1, 7))
  protected val five = Entry("Five", List("Frank"), List("support"), PublishDate(1999, 5, 5))
  protected val six = Entry("Six", List("George"), Nil, PublishDate(1999))
  protected val seven = Entry("Seven", List("Bob"), Nil, PublishDate())

  specName should {
    "initialize the database" in {
      db.init.succeed
    }
    "verify the database is empty" in {
      db.entries.transaction { implicit transaction =>
        db.entries.count.map(_ should be(0))
      }
    }
    "insert the records" in {
      db.entries.transaction { implicit transaction =>
        db.entries.insert(List(one, two, three, four, five, six, seven)).map(_ should not be None)
      }
    }
    "list author facets" in {
      db.entries.transaction { implicit transaction =>
        db.entries.query
          .facet(_.authorsFacet)
          .docs
          .limit(1)
          .search
          .map { results =>
            val authorsResult = results.facet(_.authorsFacet)
            authorsResult.childCount should be(6)
            authorsResult.totalCount should be(8)
            authorsResult.values.map(_.value) should be(List("Bob", "Lisa", "James", "Susan", "Frank", "George"))
            authorsResult.values.map(_.count) should be(List(2, 2, 1, 1, 1, 1))
          }
      }
    }
    "list all publishDate facets" in {
      db.entries.transaction { implicit transaction =>
        db.entries.query
          .facet(_.publishDateFacet)
          .docs
          .search
          .map { results =>
            val publishDateResult = results.facet(_.publishDateFacet)
            publishDateResult.values.map(_.value) should be(List("2010", "2012", "1999"))
            publishDateResult.childCount should be(4)
            publishDateResult.values.map(_.count) should be(List(2, 2, 2))
            publishDateResult.totalCount should be(6)
          }
      }
    }
    "list all support@one.com keyword facets" in {
      db.entries.transaction { implicit transaction =>
        db.entries.query
          .filter(_.keywords has "support@one.com")
          .facet(_.keywordsFacet)
          .docs
          .search
          .map { results =>
            val keywordsResult = results.facet(_.keywordsFacet)
            keywordsResult.childCount should be(2)
            keywordsResult.totalCount should be(3)
            keywordsResult.values.map(_.value) should be(List("support@one.com", "support@two.com"))
            keywordsResult.values.map(_.count) should be(List(2, 1))
          }
      }
    }
    "modify a record" in {
      db.entries.transaction { implicit transaction =>
        db.entries.upsert(five.copy(name = "Cinco")).succeed
      }
    }
    "list all results for 2010" in {
      db.entries.transaction { implicit transaction =>
        db.entries.query
          .filter(_.publishDateFacet.drillDown("2010"))
          .facet(_.authorsFacet)
          .facet(_.publishDateFacet, path = List("2010"))
          .search
          .map { results =>
            val authorResult = results.facet(_.authorsFacet)
            authorResult.childCount should be(3)
            authorResult.totalCount should be(3)
            authorResult.values.map(_.value) should be(List("Bob", "James", "Lisa"))
            authorResult.values.map(_.count) should be(List(1, 1, 1))
            val publishResult = results.facet(_.publishDateFacet)
            publishResult.childCount should be(1)
            publishResult.totalCount should be(2)
            publishResult.values.map(_.value) should be(List("10"))
            publishResult.values.map(_.count) should be(List(2))
          }
      }
    }
    "exclude all results for 2010" in {
      db.entries.transaction { implicit transaction =>
        db.entries.query
          .facet(_.authorsFacet)
          .facet(_.publishDateFacet)
          .filter(_.builder.mustNot(_.publishDateFacet.drillDown("2010")))
          .search
          .map { results =>
            val authorResult = results.facet(_.authorsFacet)
            authorResult.childCount should be(5)
            authorResult.totalCount should be(5)
            authorResult.values.map(_.value) should be(List("Bob", "Lisa", "Susan", "Frank", "George"))
            authorResult.values.map(_.count) should be(List(1, 1, 1, 1, 1))
            val publishResult = results.facet(_.publishDateFacet)
            publishResult.childCount should be(3)
            publishResult.totalCount should be(4)
            publishResult.values.map(_.value) should be(List("2012", "1999"))
            publishResult.values.map(_.count) should be(List(2, 2))
          }
      }
    }
    "list all results for 2010/10" in {
      db.entries.transaction { implicit transaction =>
        db.entries.query
          .facet(_.authorsFacet)
          .facet(_.publishDateFacet, path = List("2010", "10"))
          .filter(_.publishDateFacet.drillDown("2010", "10"))
          .search
          .map { results =>
            val authorResult = results.facet(_.authorsFacet)
            authorResult.childCount should be(3)
            authorResult.totalCount should be(3)
            authorResult.values.map(_.value) should be(List("Bob", "James", "Lisa"))
            authorResult.values.map(_.count) should be(List(1, 1, 1))
            val publishResult = results.facet(_.publishDateFacet)
            publishResult.childCount should be(2)
            publishResult.totalCount should be(2)
            publishResult.values.map(_.value) should be(List("15", "20"))
            publishResult.values.map(_.count) should be(List(1, 1))
          }
      }
    }
    "list all results for 2010/10/20" in {
      db.entries.transaction { implicit transaction =>
        db.entries.query
          .facet(_.authorsFacet)
          .facet(_.publishDateFacet, path = List("2010", "10", "20"))
          .filter(_.publishDateFacet.drillDown("2010", "10", "20"))
          .search
          .map { results =>
            val authorResult = results.facet(_.authorsFacet)
            authorResult.childCount should be(1)
            authorResult.totalCount should be(1)
            authorResult.values.map(_.value) should be(List("Lisa"))
            authorResult.values.map(_.count) should be(List(1))
            val publishResult = results.facet(_.publishDateFacet)
            publishResult.childCount should be(1)
            publishResult.totalCount should be(0)
            publishResult.values should be(Nil)
          }
      }
    }
    "show only results for 1999" in {
      db.entries.transaction { implicit transaction =>
        db.entries.query
          .facet(_.authorsFacet)
          .facet(_.publishDateFacet, path = List("1999"))
          .filter(_.publishDateFacet.drillDown("1999").onlyThisLevel)
          .search
          .map { results =>
            val authorResult = results.facet(_.authorsFacet)
            authorResult.childCount should be(1)
            authorResult.totalCount should be(1)
            authorResult.values.map(_.value) should be(List("George"))
            authorResult.values.map(_.count) should be(List(1))
            val publishResult = results.facet(_.publishDateFacet)
            publishResult.childCount should be(1)
            publishResult.totalCount should be(0)
            publishResult.values should be(Nil)
          }
      }
    }
    "show all results for support@two.com" in {
      db.entries.transaction { implicit transaction =>
        db.entries.query
          .filter(_.keywordsFacet.drillDown("support@two.com"))
          .search
          .flatMap { results =>
            results.list.map(_.map(_.name).toSet should be(Set("One", "Three")))
          }
      }
    }
    "show all results for support@three.com or support" in {
      db.entries.transaction { implicit transaction =>
        db.entries.query
          .filter(_.builder
            .should(_.keywordsFacet.drillDown("support@three.com"))
            .should(_.keywordsFacet.drillDown("support"))
          )
          .stream
          .toList
          .map { list =>
            list.map(_.name).toSet should be(Set("Four", "Cinco"))
          }
      }
    }
    "remove a keyword from One" in {
      db.entries.transaction { implicit transaction =>
        db.entries.upsert(one.copy(keywords = List("support@one.com"))).succeed
      }
    }
    "show all results for support@two.com excluding updated" in {
      db.entries.transaction { implicit transaction =>
        db.entries.query
          .filter(_.keywordsFacet.drillDown("support@two.com"))
          .stream
          .toList
          .map { results =>
            results.map(_.name).toSet should be(Set("Three"))
          }
      }
    }
    "show only top-level results without a publish date" in {
      db.entries.transaction { implicit transaction =>
        db.entries.query
          .facet(_.authorsFacet)
          .facet(_.publishDateFacet)
          .filter(_.publishDateFacet.drillDown().onlyThisLevel)
          .search
          .map { results =>
            val authorResult = results.facet(_.authorsFacet)
            authorResult.totalCount should be(1)
            authorResult.childCount should be(1)
            authorResult.values.map(_.value) should be(List("Bob"))
            authorResult.values.map(_.count) should be(List(1))
            val publishResult = results.facet(_.publishDateFacet)
            publishResult.childCount should be(1)
            publishResult.totalCount should be(0)
          }
      }
    }
    "delete a facets document" in {
      db.entries.transaction { implicit transaction =>
        db.entries.delete(four._id).succeed
      }
    }
    "query all documents verifying deletion of Four" in {
      db.entries.transaction { implicit transaction =>
        db.entries.query.facet(_.authorsFacet).search.map { results =>
          results.getFacet(_.publishDateFacet) should be(None)
          val authorResult = results.facet(_.authorsFacet)
          authorResult.values.map(_.value) should be(List("Bob", "Lisa", "James", "Frank", "George"))
          authorResult.values.map(_.count) should be(List(2, 2, 1, 1, 1))
        }
      }
    }
    "truncate the collection" in {
      db.entries.transaction { implicit transaction =>
        db.entries.truncate().map(_ should be(6))
      }
    }
    "dispose the database" in {
      db.dispose.succeed
    }
  }

  def storeManager: StoreManager

  class DB extends LightDB {
    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val entries: Collection[Entry, Entry.type] = collection(Entry)

    override def storeManager: StoreManager = spec.storeManager
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Entry(name: String,
                   authors: List[String],
                   keywords: List[String],
                   publishDate: PublishDate,
                   _id: Id[Entry] = Entry.id()) extends Document[Entry]

  object Entry extends DocumentModel[Entry] with JsonConversion[Entry] {
    override implicit val rw: RW[Entry] = RW.gen

    val name: I[String] = field.index("name", (e: Entry) => e.name)
    val authors: F[List[String]] = field("authors", (e: Entry) => e.authors)
    val keywords: I[List[String]] = field.index("keywords", (e: Entry) => e.keywords)
    val publishDate: F[PublishDate] = field("publishDate", (e: Entry) => e.publishDate)

    val authorsFacet: FF = field.facet("authorsFacet", _.authors.map(a => FacetValue(a)), FacetConfig(multiValued = true))
    val keywordsFacet: FF = field.facet("keywordsFacet", _.keywords.map(k => FacetValue(k)), FacetConfig(multiValued = true))
    val publishDateFacet: FF = field.facet("publishDateFacet", doc => List(FacetValue(List(doc.publishDate.year, doc.publishDate.month, doc.publishDate.day).flatMap(i => if (i == -1) None else Some(i)).map(_.toString))), FacetConfig(hierarchical = true))
  }

  case class PublishDate(year: Int = -1, month: Int = -1, day: Int = -1)

  object PublishDate {
    implicit val rw: RW[PublishDate] = RW.gen
  }
}
