package spec

import lightdb.LightDB
import lightdb.file.FileStorage
import lightdb.store.prefix.PrefixScanningStoreManager
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Stream}

import java.nio.charset.StandardCharsets
import java.nio.file.Path

abstract class AbstractFileStorageSpec[SM <: PrefixScanningStoreManager](val storeManager: SM,
                                                                         dbDirectory: Option[Path] = None) extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected val chunkSize: Int = 5

  protected val firstChunks: List[Array[Byte]] = List(
    "Hello".getBytes(StandardCharsets.UTF_8),
    " RocksDB".getBytes(StandardCharsets.UTF_8),
    " Streaming!".getBytes(StandardCharsets.UTF_8)
  )
  protected val secondChunks: List[Array[Byte]] = List(
    "Second".getBytes(StandardCharsets.UTF_8),
    " File".getBytes(StandardCharsets.UTF_8)
  )
  protected val thirdChunks: List[Array[Byte]] = List(
    "Third-File-Content".getBytes(StandardCharsets.UTF_8)
  )

  private val resolvedDirectory: Option[Path] =
    dbDirectory.orElse(Some(Path.of(s"db/${getClass.getSimpleName}")))

  private object DB extends LightDB {
    override type SM = spec.storeManager.type
    override val storeManager: SM = spec.storeManager
    override lazy val directory: Option[Path] = resolvedDirectory

    val fs: FileStorage[_] = FileStorage(this)

    override protected def truncateOnInit: Boolean = true
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  private var firstFileId: String = _
  private var secondFileId: String = _
  private var thirdFileId: String = _
  private val expectedChunkCount: Int = firstChunks.map(ch => if ch.isEmpty then 0 else ((ch.length + chunkSize - 1) / chunkSize)).sum
  private val expectedFirst: Array[Byte] = firstChunks.foldLeft(Array.emptyByteArray)(_ ++ _)
  private val expectedSecond: Array[Byte] = secondChunks.foldLeft(Array.emptyByteArray)(_ ++ _)
  private val expectedThird: Array[Byte] = thirdChunks.foldLeft(Array.emptyByteArray)(_ ++ _)

  s"${getClass.getSimpleName}" should {
    "init" in {
      DB.init.succeed
    }
    "write a file" in {
      DB.fs.put("example.txt", Stream.emits(firstChunks), chunkSize).map { meta =>
        firstFileId = meta.fileId
      }.succeed
    }
    "write additional files" in {
      for
        meta2 <- DB.fs.put("second.txt", Stream.emits(secondChunks), chunkSize)
        meta3 <- DB.fs.put("third.bin", Stream.emits(thirdChunks), chunkSize)
        _ = secondFileId = meta2.fileId
        _ = thirdFileId = meta3.fileId
      yield succeed
    }
    "get a file with expected chunking" in {
      DB.fs.get(firstFileId).map { metaOpt =>
        val meta = metaOpt.get
        meta.totalChunks shouldBe expectedChunkCount
        meta.size shouldBe expectedFirst.length
      }
    }
    "list the uploaded files" in {
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
    "delete the uploaded first file" in {
      for
        _ <- DB.fs.delete(firstFileId)
        after <- DB.fs.get(firstFileId)
      yield after should be(None)
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

