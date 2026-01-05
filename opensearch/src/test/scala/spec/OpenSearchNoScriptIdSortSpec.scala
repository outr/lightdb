package spec

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.query.OpenSearchSearchBuilder
import lightdb.{Sort, SortDirection}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * Regression: avoid `_script` sorts for Id-like fields (slow path).
 *
 * By default, OpenSearchSearchBuilder should sort Id-like string fields using doc-values (`<field>.keyword`),
 * and never emit `_script` sorts unless explicitly enabled by config.
 */
class OpenSearchNoScriptIdSortSpec extends AnyWordSpec with Matchers {
  case class Parent(_id: Id[Parent] = Parent.id()) extends Document[Parent]
  object Parent extends DocumentModel[Parent] with JsonConversion[Parent] {
    override implicit val rw: RW[Parent] = RW.gen
  }

  case class Child(parentId: Id[Parent], _id: Id[Child] = Child.id()) extends Document[Child]
  object Child extends DocumentModel[Child] with JsonConversion[Child] {
    override implicit val rw: RW[Child] = RW.gen
    val parentId: I[Id[Parent]] = field.index("parentId", _.parentId)
  }

  "OpenSearchSearchBuilder id sorts" should {
    "not emit _script sorts by default for Id-like fields" in {
      val sb = new OpenSearchSearchBuilder[Child, Child.type](
        model = Child,
        allowScriptSorts = false
      )
      val sorts = sb.sortsToDsl(List(Sort.ByField(Child.parentId, SortDirection.Ascending)))
      val json = fabric.io.JsonFormatter.Compact(fabric.arr(sorts: _*))
      json.contains("\"_script\"") shouldBe false
      json.contains("parentId.keyword") shouldBe true
    }

    "emit _script sorts only when allowScriptSorts=true" in {
      val sb = new OpenSearchSearchBuilder[Child, Child.type](
        model = Child,
        allowScriptSorts = true
      )
      val sorts = sb.sortsToDsl(List(Sort.ByField(Child.parentId, SortDirection.Ascending)))
      val json = fabric.io.JsonFormatter.Compact(fabric.arr(sorts: _*))
      json.contains("\"_script\"") shouldBe true
    }
  }
}

