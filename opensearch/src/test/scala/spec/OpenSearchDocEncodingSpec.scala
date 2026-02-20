package spec

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.client.OpenSearchConfig
import lightdb.opensearch.OpenSearchDocEncoding
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

@EmbeddedTest
class OpenSearchDocEncodingSpec extends AnyWordSpec with Matchers {
  case class Inner(code: String)
  object Inner {
    implicit val rw: RW[Inner] = RW.gen
  }

  case class Outer(children: List[Inner])
  object Outer {
    implicit val rw: RW[Outer] = RW.gen
  }

  case class Entry(outers: List[Outer], _id: Id[Entry] = Entry.id()) extends Document[Entry]
  object Entry extends DocumentModel[Entry] with JsonConversion[Entry] {
    override implicit val rw: RW[Entry] = RW.gen

    trait Outers extends Nested[List[Outer]]

    val outers = field.index.nested[Outers](_.outers)
    val childCode = field("outers.children.code", _.outers.headOption.flatMap(_.children.headOption.map(_.code)).getOrElse(""))
  }

  "OpenSearchDocEncoding.prepareForIndexing" should {
    "avoid emitting dotted keys that conflict with array/object roots" in {
      val config = OpenSearchConfig(baseUrl = "http://localhost:9200")
      val doc = Entry(outers = List(Outer(children = List(Inner("x")))))
      val prepared = OpenSearchDocEncoding.prepareForIndexing(
        doc = doc,
        storeName = "Entry",
        fields = Entry.fields,
        config = config
      )

      prepared.source.asObj.value.contains("outers.children.code") shouldBe false
      prepared.source.asObj.value.contains("outers") shouldBe true
      prepared.source.asObj.value("outers").asArr.value.nonEmpty shouldBe true
    }
  }
}
