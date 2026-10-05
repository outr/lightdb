package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.lucene.index.Index
import lightdb.lucene.{LuceneDurability, LuceneStore, LuceneStoreManager}
import lightdb.rocksdb.RocksDBStore
import lightdb.store.split.{SplitCollection, SplitStoreManager}
import lightdb.upgrade.DatabaseUpgrade

import java.nio.file.Path

/** The database [[LuceneDeferredDurabilityCrashSpec]] kills a process in the middle of writing to, and reopens. */
class CrashDB(dir: Path, durability: LuceneDurability) extends LightDB {
  override type SM = SplitStoreManager[RocksDBStore.type, LuceneStoreManager]
  override val storeManager: SM = SplitStoreManager(RocksDBStore, LuceneStore.withDurability(durability))
  override def name: String = "CrashDB"
  override lazy val directory: Option[Path] = Some(dir)
  val items: SplitCollection[CrashItem, CrashItem.type, RocksDBStore[CrashItem, CrashItem.type], LuceneStore[CrashItem, CrashItem.type]] =
    store(CrashItem)()
  override def upgrades: List[DatabaseUpgrade] = Nil

  def index: Index = items.searching.index
  def marker: Path = items.searching.path.get.resolve(Index.UncommittedMarker)
}

case class CrashItem(name: String, text: String, _id: Id[CrashItem]) extends Document[CrashItem]

object CrashItem extends DocumentModel[CrashItem] with JsonConversion[CrashItem] {
  override implicit val rw: RW[CrashItem] = RW.gen
  val name: I[String] = field.index(_.name)
  val text: T = field.tokenized(_.text)

  def apply(name: String, id: String): CrashItem = CrashItem(name, s"text of $name for $id", Id[CrashItem](id))

  /** The sequence the writer process runs, transaction `i` of it:
    *  - every 7th transaction writes into the `f` documents and then fails;
    *  - every other one inserts `d<i>`, renames `d<i-1>`, and on an even `i` deletes `d<i-2>`.
    * So `d<j>` exists once transactions up to `j + 2` have committed exactly when it was inserted and not deleted, under
    * the name [[expectedName]]. */
  def fails(i: Int): Boolean = i % 7 == 0
  def deletes(i: Int): Boolean = !fails(i) && i % 2 == 0
  def expectedName(j: Int): Option[String] =
    if fails(j) || deletes(j + 2) then None
    else if fails(j + 1) then Some(s"n$j")
    else Some(s"n$j-u")
}
