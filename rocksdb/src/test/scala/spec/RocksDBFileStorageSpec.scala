package spec

import lightdb.LightDB
import lightdb.file.FileStorage
import lightdb.rocksdb.RocksDBStore
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Stream, Task}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Comparator

@EmbeddedTest
class RocksDBFileStorageSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with BeforeAndAfterAll {
  private val chunkSize = 5
  private val inputChunks = List(
    "Hello".getBytes(StandardCharsets.UTF_8),
    " RocksDB".getBytes(StandardCharsets.UTF_8),
    " Streaming!".getBytes(StandardCharsets.UTF_8)
  )

  private val dbPath: Path = Path.of("db/RocksDBFileStorageSpec")

  private object DB extends LightDB {
    override type SM = RocksDBStore.type
    override val storeManager: RocksDBStore.type = RocksDBStore
    override lazy val directory: Option[Path] = Some(dbPath)

    val fs: FileStorage[_] = FileStorage(this)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  private var fileId: String = _
  private val expectedChunkCount: Int = inputChunks.map(ch => if (ch.isEmpty) 0 else ((ch.length + chunkSize - 1) / chunkSize)).sum
  private val expectedContent: Array[Byte] = inputChunks.foldLeft(Array.emptyByteArray)(_ ++ _)

  "file storage on RocksDB" should {
    "init" in {
      DB.init.succeed
    }
    "write a file" in {
      DB.fs.put("example.txt", Stream.emits(inputChunks), chunkSize).map { meta =>
        fileId = meta.fileId
      }.succeed
    }
    "get a file with expected chunking" in {
      DB.fs.get(fileId).map { metaOpt =>
        val meta = metaOpt.get
        meta.totalChunks shouldBe expectedChunkCount
        meta.size shouldBe expectedContent.length
      }
    }
    "list the uploaded file" in {
      DB.fs.list.map { listed =>
        listed.map(_.fileId) should contain(fileId)
      }
    }
    "read back the original bytes" in {
      DB.fs.readAll(fileId).map { data =>
        data.foldLeft(Array.emptyByteArray)(_ ++ _) shouldBe expectedContent
      }
    }
    "delete the uploaded file" in {
      for {
        _ <- DB.fs.delete(fileId)
        after <- DB.fs.get(fileId)
      } yield after should be(None)
    }
    "dispose" in {
      DB.dispose.succeed
    }
  }

  private def deleteDirectoryIfExists(path: Path): Unit = {
    if (Files.exists(path)) {
      Files.walk(path)
        .sorted(Comparator.reverseOrder())
        .forEach(Files.delete(_))
    }
  }
}

