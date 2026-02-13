package spec

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.filter.{Condition, Filter, FilterClause, QueryOptimizer}
import lightdb.filter.*
import lightdb.id.Id
import lightdb.spatial.Point
import lightdb.distance.Distance
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

@EmbeddedTest
class NestedFilterCoreSpec extends AnyWordSpec with Matchers {
  case class Item(name: String, score: Double)
  object Item {
    implicit val rw: RW[Item] = RW.gen
  }

  case class Attr(key: String, percent: Double)
  object Attr {
    implicit val rw: RW[Attr] = RW.gen
  }

  case class Doc(items: List[Item], _id: Id[Doc] = Doc.id()) extends Document[Doc]
  object Doc extends DocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen
    val itemName = field.index("items.name", _.items.map(_.name))
    val itemScore = field.index("items.score", _.items.map(_.score))
    val itemsNested: String = nestedPath("items")
  }

  case class Entry(attrs: List[Attr], _id: Id[Entry] = Entry.id()) extends Document[Entry]
  object Entry extends DocumentModel[Entry] with JsonConversion[Entry] {
    override implicit val rw: RW[Entry] = RW.gen
    trait Attrs extends Nested[List[Attr]] {
      val key: NP[String]
      val percent: NP[Double]
    }
    val attrs: N[Attrs] = field.index.nested[Attrs](_.attrs)
  }

  "Nested filter core DSL" should {
    "construct a nested filter with same-element semantics" in {
      val nested = Doc.nested("items")(_ =>
        Filter.Equals[Doc, String]("name", "alpha") &&
          Filter.RangeDouble[Doc]("score", Some(0.5), None)
      ).asInstanceOf[Filter.Nested[Doc]]

      nested.path shouldBe "items"
      nested.semantics shouldBe Filter.NestedSemantics.SameElementAll
    }

    "optimize inner filters without dropping nested scope" in {
      val inner = Filter.Multi[Doc](minShould = 1, filters = List(
        FilterClause(
          filter = Filter.Multi[Doc](minShould = 1, filters = List(
            FilterClause(Filter.Equals[Doc, String]("name", "alpha"), Condition.Must, None)
          )),
          condition = Condition.Must,
          boost = None
        )
      ))

      val optimized = QueryOptimizer.optimize(Filter.Nested[Doc]("items", inner)).asInstanceOf[Filter.Nested[Doc]]
      optimized.path shouldBe "items"
      optimized.filter shouldBe a[Filter.Multi[_]]
    }

    "expose inferred nested accessors without an explicit access trait" in {
      val nested = Entry.attrs
        .nested { attrs =>
          attrs.key === "tract-a" && attrs.percent >= 0.5
        }
        .asInstanceOf[Filter.Nested[Entry]]

      nested.path shouldBe "attrs"
      nested.semantics shouldBe Filter.NestedSemantics.SameElementAll
    }

    "fail fast when fallback nested inner filters are unsupported" in {
      val unsupported = Entry.attrs
        .nested { _ =>
          Filter.Distance[Entry]("percent", Point(0.0, 0.0), Distance(1.0))
        }
      val ex = intercept[UnsupportedOperationException] {
        NestedQuerySupport.validateFallbackCompatible(Some(unsupported))
      }
      ex.getMessage should include("does not support Distance inside nested filters")
    }

    "stripNested should relax minShould to 0 when nested clause is removed from OR" in {
      // Test for regression: nested clauses in Should (OR) conditions must relax minShould
      // when stripped to ensure the broad filter remains a superset
      val nestedFilter = Filter.Nested[Doc]("items", Filter.Equals[Doc, String]("name", "alpha"))
      val otherFilter = Filter.Equals[Doc, String]("other", "value")
      
      // Create an OR filter: nested OR other
      val orFilter = Filter.Multi[Doc](
        minShould = 1,
        filters = List(
          FilterClause(nestedFilter, Condition.Should, None),
          FilterClause(otherFilter, Condition.Should, None)
        )
      )
      
      val stripped = NestedQuerySupport.stripNested(orFilter)
      
      stripped should be(defined)
      val result = stripped.get.asInstanceOf[Filter.Multi[Doc]]
      result.minShould shouldBe 0  // Should be relaxed to 0, not kept at 1
      result.filters should have size 1
      result.filters.head.filter shouldBe otherFilter
    }

    "stripNested should handle nested-only OR filters" in {
      // When all clauses in an OR are nested, stripping should return None
      val nestedFilter1 = Filter.Nested[Doc]("items", Filter.Equals[Doc, String]("name", "alpha"))
      val nestedFilter2 = Filter.Nested[Doc]("items", Filter.Equals[Doc, String]("name", "beta"))
      
      val orFilter = Filter.Multi[Doc](
        minShould = 1,
        filters = List(
          FilterClause(nestedFilter1, Condition.Should, None),
          FilterClause(nestedFilter2, Condition.Should, None)
        )
      )
      
      val stripped = NestedQuerySupport.stripNested(orFilter)
      stripped should be(None)
    }

    "stripNested should preserve minShould for AND filters when nested clauses are removed" in {
      // Test that AND filters don't get their minShould modified
      val nestedFilter = Filter.Nested[Doc]("items", Filter.Equals[Doc, String]("name", "alpha"))
      val otherFilter = Filter.Equals[Doc, String]("other", "value")
      
      val andFilter = Filter.Multi[Doc](
        minShould = 0,
        filters = List(
          FilterClause(nestedFilter, Condition.Must, None),
          FilterClause(otherFilter, Condition.Must, None)
        )
      )
      
      val stripped = NestedQuerySupport.stripNested(andFilter)
      
      stripped should be(defined)
      val result = stripped.get.asInstanceOf[Filter.Multi[Doc]]
      result.minShould shouldBe 0  // Should remain 0 for AND
      result.filters should have size 1
      result.filters.head.filter shouldBe otherFilter
    }
  }
}

