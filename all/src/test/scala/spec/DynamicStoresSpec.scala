package spec

import fabric.rw._
import lightdb.chroniclemap.ChronicleMapStore
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.upgrade.DatabaseUpgrade
import lightdb.LightDB
import lightdb.store.multi.MultiStore
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

@EmbeddedTest
class DynamicStoresSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  "Dynamic Stores" should {
    "initialize the database" in {
      DB.init.succeed
    }
    "store a user in users1" in {
      DB.users1.t.insert(User("A", 1)).succeed
    }
    "store a user in users2" in {
      DB.users2.t.insert(User("B", 2)).succeed
    }
    "verify only one user in users1" in {
      DB.users1.t.list.map { users =>
        users.map(_.name) should be(List("A"))
      }
    }
    "verify only one user in users2" in {
      DB.users2.t.list.map { users =>
        users.map(_.name) should be(List("B"))
      }
    }
    "user a multi-transaction" in {
      DB.users.transaction { txn =>
        for {
          users1 <- txn("users1").stream.toList
          users2 <- txn("users2").stream.toList
          users = users1 ::: users2
        } yield users.map(_.name) should be(List("A", "B"))
      }
    }
    "truncate the database" in {
      DB.truncate().succeed
    }
    "dispose" in {
      DB.dispose.succeed
    }
  }

  object DB extends LightDB {
    override type SM = ChronicleMapStore.type
    override val storeManager: ChronicleMapStore.type = ChronicleMapStore

    override lazy val directory: Option[Path] = Some(Path.of("db/DynamicStoresSpec"))

    val users: MultiStore[User, User.type, S[User, User.type]#TX, S[User, User.type], String] =
      multiStore(User, "users1", "users2")

    lazy val users1: S[User, User.type] = users("users1")
    lazy val users2: S[User, User.type] = users("users2")

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class User(name: String, age: Int, _id: Id[User] = User.id()) extends Document[User]

  object User extends DocumentModel[User] with JsonConversion[User] {
    override implicit val rw: RW[User] = RW.gen

    val name: F[String] = field("name", _.name)
    val age: F[Int] = field("age", _.age)
  }
}