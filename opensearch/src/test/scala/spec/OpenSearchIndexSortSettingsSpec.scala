package spec

import fabric._
import fabric.rw._
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.opensearch.OpenSearchTemplates
import lightdb.opensearch.client.OpenSearchConfig
import lightdb.time.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

@EmbeddedTest
class OpenSearchIndexSortSettingsSpec extends AnyWordSpec with Matchers {
  case class Doc(unifiedEntityId: String,
                 created: Timestamp = Timestamp(),
                 modified: Timestamp = Timestamp(),
                 _id: Id[Doc] = Id[Doc]("1")) extends RecordDocument[Doc]

  object Doc extends RecordDocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen
    val unifiedEntityId: I[String] = field.index("unifiedEntityId", _.unifiedEntityId)
  }

  "OpenSearchTemplates.indexBody" should {
    "include index.sort.field/order when configured" in {
      val cfg = OpenSearchConfig(
        baseUrl = "http://localhost:9200",
        indexSortFields = List("unifiedEntityId.keyword", OpenSearchTemplates.InternalIdField),
        indexSortOrders = List("asc", "asc")
      )

      val body = OpenSearchTemplates.indexBody(
        model = Doc,
        fields = Doc.fields,
        config = cfg,
        storeName = "Doc",
        maxResultWindow = 123
      )

      val settings = body.asObj("settings").asObj
      val index = settings("index").asObj

      index("max_result_window").asInt shouldBe 123
      index("sort.field").asArr.value.toList.map(_.asString) shouldBe List("unifiedEntityId.keyword", OpenSearchTemplates.InternalIdField)
      index("sort.order").asArr.value.toList.map(_.asString) shouldBe List("asc", "asc")
    }

    "inject a mapping for <field>.keyword when index.sort.field references it but the model doesn't declare it" in {
      val cfg = OpenSearchConfig(
        baseUrl = "http://localhost:9200",
        indexSortFields = List("unifiedEntityId.keyword", OpenSearchTemplates.InternalIdField),
        indexSortOrders = List("asc", "asc")
      )

      // Pass NO fields from the model to simulate a join-parent index body that doesn't include the child field.
      val body = OpenSearchTemplates.indexBody(
        model = Doc,
        fields = Nil,
        config = cfg,
        storeName = "Doc",
        maxResultWindow = 10
      )

      val props = body.asObj("mappings").asObj("properties").asObj
      props.get("unifiedEntityId") should not be empty

      val ue = props("unifiedEntityId").asObj
      ue("type").asString shouldBe "text"
      ue("fields").asObj("keyword").asObj("type").asString shouldBe "keyword"
    }
  }
}

