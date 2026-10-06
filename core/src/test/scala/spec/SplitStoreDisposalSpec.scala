package spec

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Collection, CollectionManager, StoreMode}
import lightdb.store.split.SplitStoreManager
import lightdb.util.Disposable
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import rapid.{AsyncTaskSpec, Task}
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

class SplitStoreDisposalSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  private class Owner extends CollectionManager with Disposable {
    type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = Collection[Doc, Model]
    val closes = new AtomicInteger()
    protected def doDispose(): Task[Unit] = Task { closes.incrementAndGet(); () }
    def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB, model: Model,
      name: String, path: Option[Path], mode: StoreMode[Doc, Model]): S[Doc, Model] =
      throw new UnsupportedOperationException("The ownership test must not open a collection")
  }
  "split-store ownership" should {
    "dispose storage and search managers exactly once, including nested splits" in {
      val storage = new Owner
      val search = new Owner
      val outerSearch = new Owner
      val manager = SplitStoreManager(SplitStoreManager(storage, search), outerSearch)
      for {
        _ <- manager.dispose
        _ <- manager.dispose
        _ <- storage.dispose
      } yield {
        storage.closes.get() shouldBe 1
        search.closes.get() shouldBe 1
        outerSearch.closes.get() shouldBe 1
      }
    }
    "close a manager shared by both halves exactly once" in {
      val owner = new Owner
      SplitStoreManager(owner, owner).dispose.map { _ => owner.closes.get() shouldBe 1 }
    }
  }
}
