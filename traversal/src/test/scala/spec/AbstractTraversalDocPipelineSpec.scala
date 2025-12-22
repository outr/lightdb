package spec

import fabric.rw._
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.filter.Filter
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.traversal.pipeline.DocPipeline
import lightdb.traversal.pipeline.{LookupStages, Pipeline}
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
  def traversalStoreManager: CollectionManager

  private lazy val specName: String = getClass.getSimpleName

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = traversalStoreManager
    override lazy val directory: Option[Path] = Some(Files.createTempDirectory("lightdb-traversal-doc-pipeline-"))
    override def upgrades: List[DatabaseUpgrade] = Nil

    val people: Collection[P, P.type] = store(P, name = Some("people"))
    val pets: Collection[Pet, Pet.type] = store(Pet, name = Some("pets"))
  }

  specName should {
    "initialize" in DB.init.succeed

    "match using DocPipeline (Filter.Equals) with candidate seeding" in {
      DB.people.transaction { tx =>
        val t = tx.asInstanceOf[lightdb.traversal.store.TraversalTransaction[P, P.type]]
        for {
          _ <- t.insert(List(
            P("Alice", 10, _id = Id("a")),
            P("Bob", 20, _id = Id("b")),
            P("Alice", 30, _id = Id("c"))
          ))
          pipeline = DocPipeline.fromTransaction(
            storeName = t.store.name,
            model = t.store.model,
            tx = t
          )
          list <- pipeline.`match`(Filter.Equals[P, String]("name", "Alice")).toList
        } yield {
          list.map(_._id.value).toSet shouldBe Set("a", "c")
        }
      }
    }

    "lookupOpt (join by id) using Pipeline stages" in {
      DB.transactions(DB.people, DB.pets) { (peopleTx0, petsTx0) =>
        val peopleTx = peopleTx0.asInstanceOf[lightdb.traversal.store.TraversalTransaction[P, P.type]]
        val petsTx = petsTx0.asInstanceOf[lightdb.traversal.store.TraversalTransaction[Pet, Pet.type]]

        for {
          _ <- petsTx.insert(List(
            Pet(ownerId = Id[P]("a"), name = "Fluffy", _id = Id[Pet]("p1")),
            Pet(ownerId = Id[P]("b"), name = "Rex", _id = Id[Pet]("p2"))
          ))
          _ <- peopleTx.insert(List(
            P("Alice", 10, bestPetId = Some(Id[Pet]("p1")), _id = Id("a")),
            P("Bob", 20, bestPetId = None, _id = Id("b"))
          ))

          pipeline = DocPipeline.fromTransaction(peopleTx.store.name, peopleTx.store.model, peopleTx)
          joined <- pipeline
            .`match`(Filter.In[P, Id[P]]("_id", Seq(Id[P]("a"), Id[P]("b"))))
            .project(p => p)
            .pipe(LookupStages.lookupOpt(petsTx, (p: P) => p.bestPetId))
            .stream
            .toList
        } yield {
          val map = joined.map { case (p, pet) => p._id.value -> pet.map(_._id.value) }.toMap
          map shouldBe Map("a" -> Some("p1"), "b" -> None)
        }
      }
    }

    "lookupMany (join by foreign key) using Pipeline stages" in {
      DB.transactions(DB.people, DB.pets) { (peopleTx0, petsTx0) =>
        val peopleTx = peopleTx0.asInstanceOf[lightdb.traversal.store.TraversalTransaction[P, P.type]]
        val petsTx = petsTx0.asInstanceOf[lightdb.traversal.store.TraversalTransaction[Pet, Pet.type]]

        for {
          _ <- petsTx.insert(List(
            Pet(ownerId = Id[P]("a"), name = "Fluffy", _id = Id[Pet]("p1")),
            Pet(ownerId = Id[P]("a"), name = "Mittens", _id = Id[Pet]("p3")),
            Pet(ownerId = Id[P]("b"), name = "Rex", _id = Id[Pet]("p2"))
          ))
          _ <- peopleTx.insert(List(
            P("Alice", 10, bestPetId = Some(Id[Pet]("p1")), _id = Id("a")),
            P("Bob", 20, bestPetId = None, _id = Id("b"))
          ))

          pipeline = DocPipeline.fromTransaction(peopleTx.store.name, peopleTx.store.model, peopleTx)
          joined <- pipeline
            .`match`(Filter.In[P, Id[P]]("_id", Seq(Id[P]("a"), Id[P]("b"))))
            .project(p => p)
            .pipe(LookupStages.lookupManyField(petsTx, Pet.ownerId, (p: P) => p._id))
            .stream
            .toList
        } yield {
          val map = joined.map { case (p, pets) => p._id.value -> pets.map(_._id.value).toSet }.toMap
          map shouldBe Map("a" -> Set("p1", "p3"), "b" -> Set("p2"))
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


