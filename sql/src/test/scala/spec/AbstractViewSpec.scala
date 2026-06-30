package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.sql.SQLCollectionManager
import lightdb.upgrade.DatabaseUpgrade
import lightdb.view.*
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

abstract class AbstractViewSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected lazy val specName: String = getClass.getSimpleName
  protected var db: DB = new DB

  def storeManager: SQLCollectionManager

  private val adam = Person("Adam", 21, Person.id("adam"))
  private val brenda = Person("Brenda", 11, Person.id("brenda"))
  private val charlie = Person("Charlie", 35, Person.id("charlie"))

  private val rex = Pet("Rex", adam._id, Pet.id("rex"))
  private val milo = Pet("Milo", charlie._id, Pet.id("milo"))
  private val bo = Pet("Bo", brenda._id, Pet.id("bo"))
  private val whiskers = Pet("Whiskers", adam._id, Pet.id("whiskers"))

  specName should {
    "initialize the database" in {
      db.init.flatMap { _ =>
        db.people.transaction(_.truncate).flatMap(_ => db.pets.transaction(_.truncate))
      }.map(_ => succeed)
    }
    "insert the people" in {
      db.people.transaction(_.insert(List(adam, brenda, charlie))).map(_ => succeed)
    }
    "rebuild the adults view and report the row count" in {
      db.adults.reBuild.map(_ should be(2))
    }
    "materialize only the adults (filter + project)" in {
      db.adults.transaction { tx =>
        tx.query.toList.map { adults =>
          adults.map(_.name).toSet should be(Set("Adam", "Charlie"))
        }
      }
    }
    "carry the projected age through" in {
      db.adults.transaction { tx =>
        tx.query.toList.map { adults =>
          adults.map(a => a.name -> a.age).toMap should be(Map("Adam" -> 21, "Charlie" -> 35))
        }
      }
    }
    "insert the pets" in {
      db.pets.transaction(_.insert(List(rex, milo, bo, whiskers))).map(_ => succeed)
    }
    "rebuild the pet-owner join view" in {
      db.petOwners.reBuild.map(_ should be(4))
    }
    "materialize the join (pet -> owner name)" in {
      db.petOwners.transaction { tx =>
        tx.query.toList.map { owners =>
          owners.map(o => o.pet -> o.owner).toMap should be(
            Map("Rex" -> "Adam", "Milo" -> "Charlie", "Bo" -> "Brenda", "Whiskers" -> "Adam"))
        }
      }
    }
    "lower a multi-store join relation to native SQL (pushdown is available, not silently generic)" in {
      val sql = lightdb.sql.RelationSql.lower(db.petOwners.relation)
      Task(sql.exists(_.toUpperCase.contains("JOIN")) should be(true))
    }
    "rebuild the pet-count aggregate view" in {
      db.petCounts.reBuild.map(_ should be(3))
    }
    "materialize counts per owner (groupBy + count)" in {
      db.petCounts.transaction { tx =>
        tx.query.toList.map { counts =>
          counts.map(c => c.owner -> c.count).toMap should be(
            Map(adam._id.value -> 2, charlie._id.value -> 1, brenda._id.value -> 1))
        }
      }
    }
    "rebuild the derived-table join view (people + aggregated pet counts)" in {
      db.personPetCounts.reBuild.map(_ should be(3))
    }
    "materialize join against a derived aggregate" in {
      db.personPetCounts.transaction { tx =>
        tx.query.toList.map { rows =>
          rows.map(r => r.name -> r.petCount).toMap should be(Map("Adam" -> 2, "Charlie" -> 1, "Brenda" -> 1))
        }
      }
    }
    "rebuild the union view (people + pets)" in {
      db.namedThings.reBuild.map(_ should be(7))
    }
    "materialize the union with per-arm kind" in {
      db.namedThings.transaction { tx =>
        tx.query.toList.map { things =>
          things.count(_.kind == "person") should be(3)
          things.count(_.kind == "pet") should be(4)
          things.map(_.name).toSet should be(Set("Adam", "Brenda", "Charlie", "Rex", "Milo", "Bo", "Whiskers"))
        }
      }
    }
    "rebuild the owner-status view (leftJoin + groupBy + count-in-CASE + concat id)" in {
      db.ownerStatus.reBuild.map(_ should be(3))
    }
    "materialize a count-threshold status with a composed id" in {
      db.ownerStatus.transaction { tx =>
        tx.query.toList.map { rows =>
          rows.map(r => r.name -> r.label).toMap should be(Map("Adam" -> "loaded", "Brenda" -> "light", "Charlie" -> "light"))
          rows.find(_.name == "Adam").map(_._id.value) should be(Some(s"${adam._id.value}:status"))
        }
      }
    }
    "auto-maintain a triggered view from the people already committed (no manual reBuild)" in {
      db.liveAdults.transaction { tx =>
        tx.query.toList.map(_.map(_.name).toSet should be(Set("Adam", "Charlie")))
      }
    }
    "auto-maintain a triggered view when a dependency inserts" in {
      val dana = Person("Dana", 40, Person.id("dana"))
      for {
        _ <- db.people.transaction(_.insert(dana))
        names <- db.liveAdults.transaction(_.query.toList.map(_.map(_.name).toSet))
      } yield names should be(Set("Adam", "Charlie", "Dana"))
    }
    "auto-maintain a triggered view when a dependency deletes" in {
      for {
        _ <- db.people.transaction(_.delete(Person.id("dana")))
        names <- db.liveAdults.transaction(_.query.toList.map(_.map(_.name).toSet))
      } yield names should be(Set("Adam", "Charlie"))
    }
  }

  class DB extends LightDB {
    override type SM = SQLCollectionManager
    override val storeManager: SQLCollectionManager = spec.storeManager

    override def name: String = specName

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val people: S[Person, Person.type] = store(Person)()

    val pets: S[Pet, Pet.type] = store(Pet)()

    val adults: View[Adult, Adult.type] = View(store(Adult)(), Materialization.cachedManual) {
      from(people)
        .where(p => p(Person.age) >= 18)
        .select(p => List(
          Adult._id := p(Person._id),
          Adult.name := p(Person.name),
          Adult.age := p(Person.age)
        ))
    }

    val petOwners: View[PetOwner, PetOwner.type] = View(store(PetOwner)()) {
      from(pets)
        .join(from(people))((pet, person) => pet(Pet.owner) === person(Person._id))
        .select((pet, person) => List(
          PetOwner._id := pet(Pet._id),
          PetOwner.pet := pet(Pet.name),
          PetOwner.owner := person(Person.name)
        ))
    }

    val petCounts: View[PetCount, PetCount.type] = View(store(PetCount)()) {
      from(pets)
        .groupBy(pet => List(pet(Pet.owner)))
        .select(pet => List(
          PetCount._id := pet(Pet.owner),
          PetCount.owner := pet(Pet.owner),
          PetCount.count := count(pet(Pet.name))
        ))
    }

    val personPetCounts: View[PersonPetCount, PersonPetCount.type] = View(store(PersonPetCount)()) {
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

    val liveAdults: View[LiveAdult, LiveAdult.type] = View(store(LiveAdult)(), Materialization.Triggered) {
      from(people)
        .where(p => p(Person.age) >= 18)
        .select(p => List(
          LiveAdult._id := p(Person._id),
          LiveAdult.name := p(Person.name),
          LiveAdult.age := p(Person.age)
        ))
    }

    val ownerStatus: View[OwnerStatus, OwnerStatus.type] = View(store(OwnerStatus)()) {
      from(people, "per")
        .leftJoin(from(pets, "pet"))((per, pet) => per(Person._id) === pet(Pet.owner))
        .groupBy((per, pet) => List(per(Person._id), per(Person.name)))
        .select((per, pet) => List(
          OwnerStatus._id := concat(per(Person._id), lit(":status")),
          OwnerStatus.name := per(Person.name),
          OwnerStatus.label := caseWhen((count(pet(Pet._id)) >= lit(2)) -> lit("loaded")).otherwise(lit("light"))
        ))
    }

    val namedThings: View[NamedThing, NamedThing.type] = View(store(NamedThing)()) {
      union(
        from(people).select(p => List(
          NamedThing._id := p(Person._id), NamedThing.name := p(Person.name), NamedThing.kind := lit("person"))),
        from(pets).select(pet => List(
          NamedThing._id := pet(Pet._id), NamedThing.name := pet(Pet.name), NamedThing.kind := lit("pet")))
      )
    }

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name: I[String] = field.index(_.name)
    val age: I[Int] = field.index(_.age)
  }

  case class Adult(name: String, age: Int, _id: Id[Adult] = Adult.id()) extends Document[Adult]

  object Adult extends DocumentModel[Adult] with JsonConversion[Adult] {
    override implicit val rw: RW[Adult] = RW.gen

    val name: I[String] = field.index(_.name)
    val age: I[Int] = field.index(_.age)
  }

  case class LiveAdult(name: String, age: Int, _id: Id[LiveAdult] = LiveAdult.id()) extends Document[LiveAdult]

  object LiveAdult extends DocumentModel[LiveAdult] with JsonConversion[LiveAdult] {
    override implicit val rw: RW[LiveAdult] = RW.gen

    val name: I[String] = field.index(_.name)
    val age: I[Int] = field.index(_.age)
  }

  case class Pet(name: String, owner: Id[Person], _id: Id[Pet] = Pet.id()) extends Document[Pet]

  object Pet extends DocumentModel[Pet] with JsonConversion[Pet] {
    override implicit val rw: RW[Pet] = RW.gen

    val name: I[String] = field.index(_.name)
    val owner: I[Id[Person]] = field.index(_.owner)
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
}
