package spec

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.OpenSearchTemplates
import lightdb.opensearch.client.OpenSearchConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

@EmbeddedTest
class OpenSearchNestedMappingSpec extends AnyWordSpec with Matchers {
  case class Owner(name: String, share: Double)
  object Owner {
    implicit val rw: RW[Owner] = RW.gen
  }

  case class Parcel(owners: List[Owner], _id: Id[Parcel] = Parcel.id()) extends Document[Parcel]
  object Parcel extends DocumentModel[Parcel] with JsonConversion[Parcel] {
    override implicit val rw: RW[Parcel] = RW.gen
    val ownerName = field.index("owners.name", _.owners.map(_.name))
    val ownerShare = field.index("owners.share", _.owners.map(_.share))
    val ownersNested: String = nestedPath("owners")
  }

  "OpenSearchTemplates nested paths" should {
    "emit nested mapping type for declared model nested paths" in {
      val config = OpenSearchConfig(baseUrl = "http://localhost:9200")
      val body = OpenSearchTemplates.indexBody(Parcel, Parcel.fields, config, storeName = "Parcel")
      val owners = body
        .asObj
        .value("mappings")
        .asObj
        .value("properties")
        .asObj
        .value("owners")
        .asObj

      owners.value("type").asString shouldBe "nested"
      owners.value("dynamic").asBoolean shouldBe true
    }
  }
}

