package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.graph.{EdgeDocument, EdgeModel, ReverseEdgeDocument}
import lightdb.id.{EdgeId, Id}
import lightdb.rocksdb.RocksDBStore
import lightdb.sql.SQLiteStore
import lightdb.store.StoreManager
import lightdb.store.journal.{Journal, JournalCollectionManager, JournalStoreManager}
import lightdb.upgrade.DatabaseUpgrade

import java.nio.file.Path

/** The database [[JournalCrashSpec]] kills a process in the middle of committing to, and reopens: every store
  * journalled over `backend` (`sqlite`, a collection manager, or `rocks`, one RocksDB database per store). */
class JournalCrashDB(dir: Path, backend: String) extends LightDB {
  override type SM = StoreManager
  override val storeManager: StoreManager = backend match {
    case "sqlite" => JournalCollectionManager(SQLiteStore)
    case "rocks" => JournalStoreManager(RocksDBStore)
    case other => throw new IllegalArgumentException(s"unknown backend $other")
  }
  override def name: String = "JournalCrashDB"
  override lazy val directory: Option[Path] = Some(dir)
  override protected def disposeOnShutdown: Boolean = false

  val people: S[CrashPerson, CrashPerson.type] = store(CrashPerson)()
  val tags: S[CrashTag, CrashTag.type] = store(CrashTag)()
  val links: S[CrashLink, CrashLink.type] = store(CrashLink)()
  val reverseModel: EdgeModel[ReverseEdgeDocument[CrashLink, CrashPerson, CrashPerson], CrashPerson, CrashPerson] =
    ReverseEdgeDocument.createModel[CrashLink, CrashPerson, CrashPerson]("crashLinksReversed")
  val reversed: S[ReverseEdgeDocument[CrashLink, CrashPerson, CrashPerson], reverseModel.type] =
    reverseStore[CrashLink, CrashPerson, CrashPerson, CrashLink.type, reverseModel.type](reverseModel, links)

  override def upgrades: List[DatabaseUpgrade] = Nil

  def journal: Journal = storeManager match {
    case m: JournalStoreManager[?] => m.journal
    case m: JournalCollectionManager[?] => m.journal
    case other => throw new IllegalStateException(s"$other is not journalled")
  }
}

case class CrashPerson(name: String, _id: Id[CrashPerson]) extends Document[CrashPerson]

object CrashPerson extends DocumentModel[CrashPerson] with JsonConversion[CrashPerson] {
  override implicit val rw: RW[CrashPerson] = RW.gen
  val name: I[String] = field.index(_.name)
}

case class CrashTag(name: String, _id: Id[CrashTag]) extends Document[CrashTag]

object CrashTag extends DocumentModel[CrashTag] with JsonConversion[CrashTag] {
  override implicit val rw: RW[CrashTag] = RW.gen
  val name: I[String] = field.index(_.name)
}

case class CrashLink(_from: Id[CrashPerson], _to: Id[CrashPerson], _id: EdgeId[CrashLink, CrashPerson, CrashPerson])
  extends EdgeDocument[CrashLink, CrashPerson, CrashPerson]

object CrashLink extends EdgeModel[CrashLink, CrashPerson, CrashPerson] with JsonConversion[CrashLink] {
  override implicit val rw: RW[CrashLink] = RW.gen

  def apply(_from: Id[CrashPerson], _to: Id[CrashPerson]): CrashLink = CrashLink(_from, _to, EdgeId(_from, _to))
}
