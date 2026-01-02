package spec

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

/**
 * TDD regression spec for a LogicalNetwork symptom:
 * - `UnionBuilder` does `clearLimit.clearPageSize.stream` over EntityRecords, filtered by `partyType` and sorted by `unifiedEntityId`.
 * - On OpenSearch, if `pageSize=None` accidentally results in `size` being omitted, OpenSearch defaults to 10 hits
 *   and the stream silently truncates to 10 results.
 */
@EmbeddedTest
class OpenSearchPartyTypeClearPageSizeStreamSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  sealed trait PartyType
  object PartyType {
    implicit val rw: RW[PartyType] = RW.enumeration(List(Person, Company))
    case object Person extends PartyType
    case object Company extends PartyType
  }

  trait UE

  case class ER(partyType: PartyType,
                unifiedEntityId: Id[UE],
                _id: Id[ER] = ER.id()) extends Document[ER]
  object ER extends DocumentModel[ER] with JsonConversion[ER] {
    override implicit val rw: RW[ER] = RW.gen
    val partyType: I[PartyType] = field.index(_.partyType)
    val unifiedEntityId: I[Id[UE]] = field.index("unifiedEntityId", _.unifiedEntityId)
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val ers: OpenSearchStore[ER, ER.type] = store(ER)
  }

  "OpenSearch clearPageSize stream (LogicalNetwork / partyType scenario)" should {
    "not silently truncate streams to 10 hits when clearPageSize is used" in {
      val db = new DB

      // Build >10 distinct unifiedEntityIds for Person, with multiple ERs per UE to mimic EntityRecords.
      val personUes = (1 to 25).toList.map(i => Id[UE](s"ue_person_$i"))
      val docs = personUes.flatMap { ueId =>
        List(
          ER(partyType = PartyType.Person, unifiedEntityId = ueId, _id = Id[ER](s"${ueId.value}_a")),
          ER(partyType = PartyType.Person, unifiedEntityId = ueId, _id = Id[ER](s"${ueId.value}_b"))
        )
      } ++ List(
        ER(partyType = PartyType.Company, unifiedEntityId = Id[UE]("ue_company_1"), _id = Id[ER]("c1")),
        ER(partyType = PartyType.Company, unifiedEntityId = Id[UE]("ue_company_2"), _id = Id[ER]("c2"))
      )

      val test = for {
        _ <- db.init
        _ <- db.ers.transaction { tx =>
          tx.truncate.next(tx.insert(docs)).next(tx.commit)
        }

        // Mirror UnionBuilder's query shape: filter partyType, sort, materialize UE id, clearLimit, clearPageSize, stream.
        uniqueUes <- db.ers.transaction { tx =>
          tx.query
            .filter(_.partyType === PartyType.Person)
            .sort(
              Sort.ByField(ER.unifiedEntityId),
              Sort.ByField(ER._id)
            )
            .materialized(_ => List(ER.unifiedEntityId))
            .clearLimit
            .clearPageSize
            .stream
            .map(mi => mi(_.unifiedEntityId))
            .distinct
            .toList
        }

        _ <- db.dispose
      } yield {
        uniqueUes.length shouldBe 25
      }

      test
    }
  }
}

