package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.filter.Filter
import lightdb.id.Id
import lightdb.traversal.pipeline.DocPipeline
import lightdb.traversal.pipeline.LookupStages
import lightdb.traversal.store.{TraversalManager, TraversalTransaction}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.{Files, Path}

/**
 * Abstract spec for DocPipeline + lookup stages (Traversal-specific).
 *
 * Concrete store modules should extend this and provide `traversalStoreManager`.
 */
abstract class AbstractTraversalDocPipelineSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
{
  def traversalStoreManager: TraversalManager

  private lazy val specName: String = getClass.getSimpleName

  object DB extends LightDB {
    override type SM = TraversalManager
    override val storeManager: TraversalManager = traversalStoreManager
    override lazy val directory: Option[Path] = Some(Files.createTempDirectory("lightdb-traversal-doc-pipeline-"))
    override def upgrades: List[DatabaseUpgrade] = Nil

    val people: S[P, P.type] = store(P, name = Some("people"))
    val pets: S[Pet, Pet.type] = store(Pet, name = Some("pets"))
  }

  specName should {
    "initialize" in {
      DB.init.succeed
    }
    "match using DocPipeline (Filter.Equals) with candidate seeding" in {
      DB.people.transaction { tx =>
        for
          _ <- tx.insert(List(
            P("Alice", 10, _id = Id("a")),
            P("Bob", 20, _id = Id("b")),
            P("Alice", 30, _id = Id("c"))
          ))
          pipeline = DocPipeline.fromTransaction(tx)
          list <- pipeline.`match`(P.name === "Alice").toList
        yield {
          list.map(_._id.value).toSet shouldBe Set("a", "c")
        }
      }
    }

    "lookupOpt (join by id) using Pipeline stages" in {
      DB.people.transaction { people =>
        DB.pets.transaction { pets =>
          for
            _ <- pets.insert(List(
              Pet(ownerId = Id[P]("a"), name = "Fluffy", _id = Id[Pet]("p1")),
              Pet(ownerId = Id[P]("b"), name = "Rex", _id = Id[Pet]("p2"))
            ))
            _ <- people.insert(List(
              P("Alice", 10, bestPetId = Some(Id[Pet]("p1")), _id = Id("a")),
              P("Bob", 20, bestPetId = None, _id = Id("b"))
            ))
            pipeline = DocPipeline.fromTransaction(people)
            joined <- pipeline
              .`match`(P._id.in(Seq(Id[P]("a"), Id[P]("b"))))
              .project(p => p)
              .pipe(LookupStages.lookupOpt(pets, (p: P) => p.bestPetId))
              .stream
              .toList
          yield {
            val map = joined.map { case (p, pet) => p._id.value -> pet.map(_._id.value) }.toMap
            map shouldBe Map("a" -> Some("p1"), "b" -> None)
          }
        }
      }
    }

    "lookupMany (join by foreign key) using Pipeline stages" in {
      DB.people.transaction { people =>
        DB.pets.transaction { pets =>
          for
            _ <- pets.insert(List(
              Pet(ownerId = Id[P]("a"), name = "Fluffy", _id = Id[Pet]("p1")),
              Pet(ownerId = Id[P]("a"), name = "Mittens", _id = Id[Pet]("p3")),
              Pet(ownerId = Id[P]("b"), name = "Rex", _id = Id[Pet]("p2"))
            ))
            _ <- people.insert(List(
              P("Alice", 10, bestPetId = Some(Id[Pet]("p1")), _id = Id("a")),
              P("Bob", 20, bestPetId = None, _id = Id("b"))
            ))

            pipeline = DocPipeline.fromTransaction(people)
            joined <- pipeline
              .`match`(P._id.in(Seq(Id[P]("a"), Id[P]("b"))))
              .project(p => p)
              .pipe(LookupStages.lookupManyField(pets, Pet.ownerId, (p: P) => p._id))
              .stream
              .toList
          yield {
            val map = joined.map { case (p, pets) => p._id.value -> pets.map(_._id.value).toSet }.toMap
            map shouldBe Map("a" -> Set("p1", "p3"), "b" -> Set("p2"))
          }
        }
      }
    }
  }
}

case class P(name: String, age: Int, bestPetId: Option[Id[Pet]] = None, _id: Id[P] = Id()) extends Document[P]

object P extends DocumentModel[P] with JsonConversion[P] {
  override implicit val rw: RW[P] = RW.gen
  val name: I[String] = field.index("name", _.name)
  val age: I[Int] = field.index("age", _.age)
  val bestPetId: I[Option[Id[Pet]]] = field.index("bestPetId", _.bestPetId)
}

case class Pet(ownerId: Id[P], name: String, _id: Id[Pet] = Id()) extends Document[Pet]

object Pet extends DocumentModel[Pet] with JsonConversion[Pet] {
  override implicit val rw: RW[Pet] = RW.gen
  val ownerId: I[Id[P]] = field.index("ownerId", _.ownerId)
  val name: I[String] = field.index("name", _.name)
}


