package benchmark.jmh.impl

import fabric.rw._
import lightdb._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.id.{EdgeId, Id}
import lightdb.store._
import lightdb.upgrade.DatabaseUpgrade
import org.apache.commons.io.FileUtils
import rapid.Task

import java.io.File
import java.nio.file.Path
import scala.util.Random

case class LightDBImpl(storeManager: StoreManager) extends BenchmarkImplementation {
  private var db: LightDB = _
  private var records: Store[Record, RecordModel.type] = _
  private var links: Store[LinksTo, LinksToModel.type] = _
  private val path = new File("benchdb")

  override def init: Task[Unit] = Task {
    FileUtils.deleteDirectory(path)
    path.mkdirs()

    db = new LightDB {
      override type SM = StoreManager
      override def directory: Option[Path] = Some(path.toPath)
      override val storeManager: StoreManager = LightDBImpl.this.storeManager
      override def upgrades: List[DatabaseUpgrade] = Nil
    }

    records = db.store(RecordModel)
    links = db.store(LinksToModel)
  }

  override def insert(iterations: Int): Task[Unit] = records.transaction { tx =>
    val recs = (0 until iterations).map(_ => RecordModel.generate()).toList
    for {
      _ <- tx.insert(recs)
      _ <- links.transaction(_.insert(recs.sliding(2).collect {
        case Seq(from, to) => LinksToModel(from._id, to._id)
      }.toList))
    } yield ()
  }

  override def count: Task[Int] = records.transaction(_.count)

  override def read: Task[Int] = records.transaction { tx =>
    tx.stream.map { record =>
      require(record.key.nonEmpty)
      require(record._id.value.nonEmpty)
      1
    }.fold(0)((i1, i2) => Task.pure(i1 + i2))
  }

  override def traverse: Task[Unit] = links match {
    case pss: PrefixScanningStore[LinksTo, LinksToModel.type] => pss.transaction { tx =>
      tx.traverse.edgesFor[LinksTo, Record, Record](Id[Record]("r")).foreach(_ => ()).drain
    }
    case _ => Task.error(new UnsupportedOperationException(s"${storeManager.name} does not support prefix scanning"))
  }

  override def dispose: Task[Unit] = Task {
    db.dispose()
    FileUtils.deleteDirectory(path)
  }
}

case class Record(key: String, number: Int, _id: Id[Record]) extends Document[Record]

object RecordModel extends DocumentModel[Record] with JsonConversion[Record] {
  override implicit val rw: RW[Record] = RW.gen
  def generate(): Record = Record(
    key = Random.alphanumeric.take(10).mkString,
    number = Random.nextInt(),
    _id = Id[Record]()
  )
}

case class LinksTo(_from: Id[Record], _to: Id[Record], _id: EdgeId[LinksTo, Record, Record]) extends EdgeDocument[LinksTo, Record, Record]

object LinksToModel extends EdgeModel[LinksTo, Record, Record] with JsonConversion[LinksTo] {
  override implicit val rw: RW[LinksTo] = RW.gen
  def apply(_from: Id[Record], _to: Id[Record]): LinksTo = LinksTo(_from, _to, EdgeId(_from, _to))
}