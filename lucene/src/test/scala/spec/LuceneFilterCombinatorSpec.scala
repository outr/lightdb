package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.filter.*
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.store.{Collection, CollectionManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

/**
 * `&&` / `||` composition semantics for [[lightdb.filter.Filter.Multi]]
 * operands. Two any-of groups combined with `&&` must BOTH be
 * satisfied — flattening their Should clauses under one minShould
 * would let a match in either group satisfy the pair, silently
 * turning the AND into an OR (a token group AND'd with a tenant/space
 * group then returns the whole tenant). Symmetrically, `||` over
 * conjunction groups must not pour one side's clauses into the
 * other's Must group.
 */
@EmbeddedTest
class LuceneFilterCombinatorSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = LuceneStore
    override lazy val directory: Option[Path] = Some(Path.of("db/LuceneFilterCombinatorSpec"))
    val notes: Collection[CombinatorNote, CombinatorNote.type] = store(CombinatorNote)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  private def tokenGroup(tokens: List[String]) =
    Filter.Multi[CombinatorNote](minShould = 1, filters = tokens.map { t =>
      FilterClause(Filter.Equals(CombinatorNote.text.name, t), Condition.Should, None)
    })

  private def categoryGroup(categories: List[String]) =
    Filter.Multi[CombinatorNote](minShould = 1, filters = categories.map { c =>
      FilterClause(Filter.Equals(CombinatorNote.category.name, c), Condition.Should, None)
    })

  "Filter combinators" should {
    "initialize and seed" in {
      DB.init.flatMap { _ =>
        DB.notes.transaction { tx =>
          tx.insert(List(
            CombinatorNote("zebra sighting near the river", "a", flag = true, Id("zebra-a")),
            CombinatorNote("quiet morning walk", "a", flag = true, Id("plain-a")),
            CombinatorNote("zebra crossing painted", "c", flag = false, Id("zebra-c")),
            CombinatorNote("nothing to report", "b", flag = false, Id("plain-b"))
          )).map(_ => succeed)
        }
      }
    }

    "require BOTH any-of groups when combined with &&" in {
      DB.notes.transaction { tx =>
        tx.query.filter(_ => tokenGroup(List("zebra")) && categoryGroup(List("a", "b"))).toList.map { hits =>
          hits.map(_._id.value) shouldBe List("zebra-a")
        }
      }
    }

    "keep && semantics when a further Must clause is chained onto the pair" in {
      DB.notes.transaction { tx =>
        tx.query.filter(m =>
          tokenGroup(List("zebra")) && categoryGroup(List("a", "b")) && (m.flag === true)
        ).toList.map { hits =>
          hits.map(_._id.value) shouldBe List("zebra-a")
        }
      }
    }

    "return nothing when the token group matches nothing, even though the category group matches" in {
      DB.notes.transaction { tx =>
        tx.query.filter(_ => tokenGroup(List("unicorn")) && categoryGroup(List("a", "b"))).toList.map { hits =>
          hits shouldBe empty
        }
      }
    }

    "treat || of two conjunction groups as a true OR" in {
      DB.notes.transaction { tx =>
        tx.query.filter { m =>
          ((m.category === "a") && (m.flag === true)) || ((m.category === "b") && (m.flag === false))
        }.toList.map { hits =>
          hits.map(_._id.value).sorted shouldBe List("plain-a", "plain-b", "zebra-a")
        }
      }
    }

    "treat x || conjunction-group as a true OR" in {
      DB.notes.transaction { tx =>
        tx.query.filter { m =>
          (m.category === "c") || ((m.category === "a") && (m.flag === true))
        }.toList.map { hits =>
          hits.map(_._id.value).sorted shouldBe List("plain-a", "zebra-a", "zebra-c")
        }
      }
    }

    "truncate and dispose" in {
      DB.truncate().flatMap(_ => DB.dispose).map(_ => succeed)
    }
  }
}

case class CombinatorNote(text: String, category: String, flag: Boolean, _id: Id[CombinatorNote]) extends Document[CombinatorNote]

object CombinatorNote extends DocumentModel[CombinatorNote] with JsonConversion[CombinatorNote] {
  override implicit val rw: RW[CombinatorNote] = RW.gen
  val text: T = field.tokenized("text", _.text)
  val category: I[String] = field.index(_.category)
  val flag: I[Boolean] = field.index(_.flag)
}
