package spec

import lightdb.LightDB
import lightdb.file.FileStorage
import lightdb.rocksdb.RocksDBStore
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Stream}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Comparator

@EmbeddedTest
class RocksDBFileStorageSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with BeforeAndAfterAll {
  private val chunkSize = 5
  private val firstChunks = List(
    "Hello".getBytes(StandardCharsets.UTF_8),
    " RocksDB".getBytes(StandardCharsets.UTF_8),
    " Streaming!".getBytes(StandardCharsets.UTF_8)
  )
  private val secondChunks = List(
    "Second".getBytes(StandardCharsets.UTF_8),
    " File".getBytes(StandardCharsets.UTF_8)
  )
  private val thirdChunks = List(
    "Third-File-Content".getBytes(StandardCharsets.UTF_8)
  )

  private val dbPath: Path = Path.of("db/RocksDBFileStorageSpec")

  private object DB extends LightDB {
    override type SM = RocksDBStore.type
    override val storeManager: RocksDBStore.type = RocksDBStore
    override lazy val directory: Option[Path] = Some(dbPath)

    val fs: FileStorage[_] = FileStorage(this)

    override protected def truncateOnInit: Boolean = true

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  private var firstFileId: String = _
  private var secondFileId: String = _
  private var thirdFileId: String = _
  private val expectedChunkCount: Int = firstChunks.map(ch => if (ch.isEmpty) 0 else ((ch.length + chunkSize - 1) / chunkSize)).sum
  private val expectedFirst: Array[Byte] = firstChunks.foldLeft(Array.emptyByteArray)(_ ++ _)
  private val expectedSecond: Array[Byte] = secondChunks.foldLeft(Array.emptyByteArray)(_ ++ _)
  private val expectedThird: Array[Byte] = thirdChunks.foldLeft(Array.emptyByteArray)(_ ++ _)

  "file storage on RocksDB" should {
    "init" in {
      DB.init.succeed
    }
    "write a file" in {
      DB.fs.put("example.txt", Stream.emits(firstChunks), chunkSize).map { meta =>
        firstFileId = meta.fileId
      }.succeed
    }
    "write additional files" in {
      for {
        meta2 <- DB.fs.put("second.txt", Stream.emits(secondChunks), chunkSize)
        meta3 <- DB.fs.put("third.bin", Stream.emits(thirdChunks), chunkSize)
        _ = secondFileId = meta2.fileId
        _ = thirdFileId = meta3.fileId
      } yield succeed
    }
    "get a file with expected chunking" in {
      DB.fs.get(firstFileId).map { metaOpt =>
        val meta = metaOpt.get
        meta.totalChunks shouldBe expectedChunkCount
        meta.size shouldBe expectedFirst.length
      }
    }
    "list the uploaded file" in {
      DB.fs.list.map { listed =>
        val ids = listed.map(_.fileId).toSet
        ids should contain(firstFileId)
        ids should contain(secondFileId)
        ids should contain(thirdFileId)
      }
    }
    "read back the original bytes" in {
      DB.fs.readAll(firstFileId).map { data =>
        data.foldLeft(Array.emptyByteArray)(_ ++ _) shouldBe expectedFirst
      }
    }
    "read the second file bytes" in {
      DB.fs.readAll(secondFileId).map { data =>
        data.foldLeft(Array.emptyByteArray)(_ ++ _) shouldBe expectedSecond
      }
    }
    "read the third file bytes" in {
      DB.fs.readAll(thirdFileId).map { data =>
        data.foldLeft(Array.emptyByteArray)(_ ++ _) shouldBe expectedThird
      }
    }
    "delete the uploaded file" in {
      for {
        _ <- DB.fs.delete(firstFileId)
        after <- DB.fs.get(firstFileId)
      } yield after should be(None)
    }
    "retain other files after delete" in {
      DB.fs.list.map { listed =>
        val ids = listed.map(_.fileId).toSet
        ids should contain(secondFileId)
        ids should contain(thirdFileId)
        ids should not contain firstFileId
      }
    }
    "dispose" in {
      DB.dispose.succeed
    }
  }
}

