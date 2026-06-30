package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.store.{Store, StoreManager}
import lightdb.store.hashmap.HashMapStore
import lightdb.upgrade.DatabaseUpgrade
import lightdb.view.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

/**
 * Proves the [[RelationEngine]] (the backend-agnostic view interpreter) produces correct results on a
 * NON-SQL backend. The store here is [[HashMapStore]], which reports no [[lightdb.view.NativeViewExecutor]],
 * so every relation is forced down the generic in-memory path (no SQL pushdown). This mirrors the
 * relation shapes exercised by the SQL-backed AbstractViewSpec, asserting identical results — so a view
 * defined over any store (Lucene, Mongo, in-memory, split across databases) behaves the same as one
 * lowered to native SQL.
 */
@EmbeddedTest
class HashMapViewSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  private val adam = Person("Adam", 21, Person.id("adam"))
  private val brenda = Person("Brenda", 11, Person.id("brenda"))
  private val charlie = Person("Charlie", 35, Person.id("charlie"))

  private val rex = Pet("Rex", adam._id, Pet.id("rex"))
  private val milo = Pet("Milo", charlie._id, Pet.id("milo"))
  private val bo = Pet("Bo", brenda._id, Pet.id("bo"))
  private val whiskers = Pet("Whiskers", adam._id, Pet.id("whiskers"))

  private lazy val db = new DB

  private def execAs[T: RW](relation: Relation): Task[List[T]] =
    RelationEngine.execute(relation).map(_.map(_.as[T]))

  "the non-SQL view interpreter" should {
    "initialize and seed the in-memory (HashMap) stores" in {
      for {
        _ <- db.init
        _ <- db.people.transaction(_.insert(List(adam, brenda, charlie)))
        _ <- db.pets.transaction(_.insert(List(rex, milo, bo, whiskers)))
      } yield succeed
    }
    "drive the generic path (no native executor on a HashMap store)" in {
      Task {
        db.people.nativeViewExecutor should be(None)
        db.pets.nativeViewExecutor should be(None)
      }
    }
    "filter + project (adults only, with projected age)" in {
      execAs[Row2](db.adults).map { adults =>
        adults.map(a => a.name -> a.age).toMap should be(Map("Adam" -> 21, "Charlie" -> 35))
      }
    }
    "inner join (pet -> owner name)" in {
      execAs[PetOwnerRow](db.petOwners).map { owners =>
        owners.map(o => o.pet -> o.owner).toMap should be(
          Map("Rex" -> "Adam", "Milo" -> "Charlie", "Bo" -> "Brenda", "Whiskers" -> "Adam"))
      }
    }
    "groupBy + count (pets per owner)" in {
      execAs[CountRow](db.petCounts).map { counts =>
        counts.map(c => c.owner -> c.count).toMap should be(
          Map(adam._id.value -> 2, charlie._id.value -> 1, brenda._id.value -> 1))
      }
    }
    "join against a derived aggregate (people + pet counts)" in {
      execAs[NamePetCountRow](db.personPetCounts).map { rows =>
        rows.map(r => r.name -> r.petCount).toMap should be(Map("Adam" -> 2, "Charlie" -> 1, "Brenda" -> 1))
      }
    }
    "union (people + pets) with per-arm kind" in {
      execAs[NamedThingRow](db.namedThings).map { things =>
        things.count(_.kind == "person") should be(3)
        things.count(_.kind == "pet") should be(4)
        things.map(_.name).toSet should be(Set("Adam", "Brenda", "Charlie", "Rex", "Milo", "Bo", "Whiskers"))
      }
    }
    "leftJoin + groupBy + count-in-CASE + concat id" in {
      execAs[StatusRow](db.ownerStatus).map { rows =>
        rows.map(r => r.name -> r.label).toMap should be(
          Map("Adam" -> "loaded", "Brenda" -> "light", "Charlie" -> "light"))
        rows.find(_.name == "Adam").map(_._id) should be(Some(s"${adam._id.value}:status"))
      }
    }
  }

  class DB extends LightDB {
    override type SM = StoreManager
    override val storeManager: StoreManager = HashMapStore

    override def name: String = "HashMapViewSpec"

    lazy val directory: Option[Path] = None

    val people: Store[Person, Person.type] = store(Person)()
    val pets: Store[Pet, Pet.type] = store(Pet)()

    val adults: Relation =
      from(people)
        .where(p => p(Person.age) >= 18)
        .select(p => List(
          Adult._id := p(Person._id),
          Adult.name := p(Person.name),
          Adult.age := p(Person.age)
        ))

    val petOwners: Relation =
      from(pets)
        .join(from(people))((pet, person) => pet(Pet.owner) === person(Person._id))
        .select((pet, person) => List(
          PetOwner._id := pet(Pet._id),
          PetOwner.pet := pet(Pet.name),
          PetOwner.owner := person(Person.name)
        ))

    val petCounts: Relation =
      from(pets)
        .groupBy(pet => List(pet(Pet.owner)))
        .select(pet => List(
          PetCount._id := pet(Pet.owner),
          PetCount.owner := pet(Pet.owner),
          PetCount.count := count(pet(Pet.name))
        ))

    val personPetCounts: Relation = {
      val pc = derived(PetCount, "pc") {
        from(pets)
          .groupBy(p => List(p(Pet.owner)))
          .select(p => List(
            PetCount._id := p(Pet.owner),
            PetCount.owner := p(Pet.owner),
            PetCount.count := count(p(Pet.name))
          ))
      }
      from(people)
        .join(pc)((person, c) => person(Person._id) === c(PetCount.owner))
        .select((person, c) => List(
          PersonPetCount._id := person(Person._id),
          PersonPetCount.name := person(Person.name),
          PersonPetCount.petCount := c(PetCount.count)
        ))
    }

    val namedThings: Relation =
      union(
        from(people).select(p => List(
          NamedThing._id := p(Person._id), NamedThing.name := p(Person.name), NamedThing.kind := lit("person"))),
        from(pets).select(pet => List(
          NamedThing._id := pet(Pet._id), NamedThing.name := pet(Pet.name), NamedThing.kind := lit("pet")))
      )

    val ownerStatus: Relation =
      from(people, "per")
        .leftJoin(from(pets, "pet"))((per, pet) => per(Person._id) === pet(Pet.owner))
        .groupBy((per, pet) => List(per(Person._id), per(Person.name)))
        .select((per, pet) => List(
          OwnerStatus._id := concat(per(Person._id), lit(":status")),
          OwnerStatus.name := per(Person.name),
          OwnerStatus.label := caseWhen((count(pet(Pet._id)) >= lit(2)) -> lit("loaded")).otherwise(lit("light"))
        ))

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Person.id()) extends Document[Person]
  object Person extends DocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen
    val name: I[String] = field.index(_.name)
    val age: I[Int] = field.index(_.age)
  }

  case class Pet(name: String, owner: Id[Person], _id: Id[Pet] = Pet.id()) extends Document[Pet]
  object Pet extends DocumentModel[Pet] with JsonConversion[Pet] {
    override implicit val rw: RW[Pet] = RW.gen
    val name: I[String] = field.index(_.name)
    val owner: I[Id[Person]] = field.index(_.owner)
  }

  case class Adult(name: String, age: Int, _id: Id[Adult] = Adult.id()) extends Document[Adult]
  object Adult extends DocumentModel[Adult] with JsonConversion[Adult] {
    override implicit val rw: RW[Adult] = RW.gen
    val name: I[String] = field.index(_.name)
    val age: I[Int] = field.index(_.age)
  }

  case class PetOwner(pet: String, owner: String, _id: Id[PetOwner] = PetOwner.id()) extends Document[PetOwner]
  object PetOwner extends DocumentModel[PetOwner] with JsonConversion[PetOwner] {
    override implicit val rw: RW[PetOwner] = RW.gen
    val pet: I[String] = field.index(_.pet)
    val owner: I[String] = field.index(_.owner)
  }

  case class PetCount(owner: String, count: Int, _id: Id[PetCount] = PetCount.id()) extends Document[PetCount]
  object PetCount extends DocumentModel[PetCount] with JsonConversion[PetCount] {
    override implicit val rw: RW[PetCount] = RW.gen
    val owner: I[String] = field.index(_.owner)
    val count: I[Int] = field.index(_.count)
  }

  case class PersonPetCount(name: String, petCount: Int, _id: Id[PersonPetCount] = PersonPetCount.id()) extends Document[PersonPetCount]
  object PersonPetCount extends DocumentModel[PersonPetCount] with JsonConversion[PersonPetCount] {
    override implicit val rw: RW[PersonPetCount] = RW.gen
    val name: I[String] = field.index(_.name)
    val petCount: I[Int] = field.index(_.petCount)
  }

  case class OwnerStatus(name: String, label: String, _id: Id[OwnerStatus] = OwnerStatus.id()) extends Document[OwnerStatus]
  object OwnerStatus extends DocumentModel[OwnerStatus] with JsonConversion[OwnerStatus] {
    override implicit val rw: RW[OwnerStatus] = RW.gen
    val name: I[String] = field.index(_.name)
    val label: I[String] = field.index(_.label)
  }

  case class NamedThing(name: String, kind: String, _id: Id[NamedThing] = NamedThing.id()) extends Document[NamedThing]
  object NamedThing extends DocumentModel[NamedThing] with JsonConversion[NamedThing] {
    override implicit val rw: RW[NamedThing] = RW.gen
    val name: I[String] = field.index(_.name)
    val kind: I[String] = field.index(_.kind)
  }

  case class Row2(name: String, age: Int)
  object Row2 { implicit val rw: RW[Row2] = RW.gen }
  case class PetOwnerRow(pet: String, owner: String)
  object PetOwnerRow { implicit val rw: RW[PetOwnerRow] = RW.gen }
  case class CountRow(owner: String, count: Int)
  object CountRow { implicit val rw: RW[CountRow] = RW.gen }
  case class NamePetCountRow(name: String, petCount: Int)
  object NamePetCountRow { implicit val rw: RW[NamePetCountRow] = RW.gen }
  case class NamedThingRow(name: String, kind: String)
  object NamedThingRow { implicit val rw: RW[NamedThingRow] = RW.gen }
  case class StatusRow(_id: String, name: String, label: String)
  object StatusRow { implicit val rw: RW[StatusRow] = RW.gen }
}
