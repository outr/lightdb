package spec

import lightdb.LightDB
import lightdb.store.Collection
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import rapid.Task

import java.nio.file.Path

@EmbeddedTest
class OpenSearchParentChildSpec extends AbstractParentChildSpec with OpenSearchTestSupport with Matchers {
  override protected def parents: Collection[Parent, Parent.type] = DB.parents
  override protected def children: Collection[Child, Child.type] = DB.children
  override protected def initDb(): Task[Unit] = DB.init
  override protected def truncateDb(): Task[Unit] = DB.truncate()
  override protected def disposeDb(): Task[Unit] = DB.dispose

  override protected def indexParentsAndChildren(parentsData: List[Parent], childrenData: List[Child]): Task[Unit] = {
    DB.parents.transaction(_.insert(parentsData))
      .next(DB.children.transaction(_.insert(childrenData)))
      .unit
  }

  object DB extends LightDB {
    override type SM = lightdb.store.CollectionManager
    override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore

    override def name: String = getClass.getSimpleName
    override lazy val directory: Option[Path] = Some(Path.of(s"db/${getClass.getSimpleName}"))

    val parents: Collection[Parent, Parent.type] = store(Parent)
    val children: Collection[Child, Child.type] = store(Child)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
