package spec

import fabric.rw._
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.field.Field._
import lightdb.id.Id
import lightdb.opensearch.{OpenSearchQuerySyntax, OpenSearchStore}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort, SortDirection}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

@EmbeddedTest
class OpenSearchGroupingSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  import OpenSearchQuerySyntax._

  case class GroupedRecord(name: String,
                           groupId: String,
                           weight: Int,
                           created: Timestamp = Timestamp(),
                           modified: Timestamp = Timestamp(),
                           _id: Id[GroupedRecord] = GroupedRecord.id()) extends RecordDocument[GroupedRecord]

  object GroupedRecord extends RecordDocumentModel[GroupedRecord] with JsonConversion[GroupedRecord] {
    override implicit val rw: RW[GroupedRecord] = RW.gen

    val name: I[String] = field.index(_.name)
    val groupId: I[String] = field.index(_.groupId)
    val weight: I[Int] = field.index(_.weight)

    override def map2Doc(map: Map[String, Any]): GroupedRecord =
      throw new RuntimeException("map2Doc not used in grouping spec")
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def name: String = "OpenSearchGroupingSpec"
    override lazy val directory: Option[java.nio.file.Path] = Some(java.nio.file.Path.of("db/OpenSearchGroupingSpec"))
    override def upgrades: List[DatabaseUpgrade] = Nil

    val grouped: lightdb.store.Collection[GroupedRecord, GroupedRecord.type] = store(GroupedRecord)
  }

  "OpenSearch grouping" should {
    "return the highest-weight record per group" in {
      val db = new DB
      val records = List(
        GroupedRecord("g1-low", "g1", 1),
        GroupedRecord("g1-high", "g1", 5),
        GroupedRecord("g2-low", "g2", 2),
        GroupedRecord("g2-high", "g2", 7),
        GroupedRecord("g3", "g3", 3)
      )
      val test = for
        _ <- db.init
        _ <- db.grouped.transaction(_.insert(records))
        groupedResults <- db.grouped.transaction { tx =>
          tx.query
            .sort(Sort.ByField(GroupedRecord.weight, SortDirection.Descending))
            .groupBy(_.groupId, docsPerGroup = Some(1), includeScores = true, includeTotalGroupCount = true)
        }
        _ <- db.dispose
      yield {
        val groups = groupedResults.grouped
        groups.map(_.group).toSet should be(Set("g1", "g2", "g3"))
        val topByGroup = groups.map(g => g.group -> g.results.head.name).toMap
        topByGroup("g1") should be("g1-high")
        topByGroup("g2") should be("g2-high")
        topByGroup("g3") should be("g3")
        groupedResults.totalGroups should be(Some(3))
      }
      test
    }
  }
}



