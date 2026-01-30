package spec

import lightdb.LightDB
import lightdb.lucene.LuceneStore
import lightdb.lucene.blockjoin.LuceneBlockJoinStore
import lightdb.lucene.blockjoin.LuceneBlockJoinSyntax.*
import lightdb.store.Collection
import lightdb.store.CollectionManager
import lightdb.upgrade.DatabaseUpgrade
import rapid.Task

import java.nio.file.Path

@EmbeddedTest
class LuceneBlockJoinParentChildSpec extends AbstractParentChildSpec {
  override protected def parents: Collection[Parent, Parent.type] = DB.parents
  override protected def children: Collection[Child, Child.type] = DB.childrenStoreForModelOnly
  override protected def initDb(): Task[Unit] = DB.init
  override protected def truncateDb(): Task[Unit] = DB.truncate()
  override protected def disposeDb(): Task[Unit] = DB.dispose

  override protected def indexParentsAndChildren(parentsData: List[Parent], childrenData: List[Child]): Task[Unit] = {
    parentsData.foldLeft(Task.unit) { case (acc, p) =>
      acc.next(DB.parents.indexBlock(p, childrenData.filter(_.parentId == p._id)))
    }.next(DB.parents.commitIndex()).unit
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = LuceneStore

    override def name: String = getClass.getSimpleName
    override lazy val directory: Option[Path] = Some(Path.of(s"db/${getClass.getSimpleName}"))

    val parents: LuceneBlockJoinStore[Parent, Child, Child.type, Parent.type] =
      this.blockJoinCollection[Parent, Child, Child.type, Parent.type](
        parentModel = Parent,
        name = Some("entitySearch")
      )
    val childrenStoreForModelOnly: Collection[Child, Child.type] = store(Child)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
