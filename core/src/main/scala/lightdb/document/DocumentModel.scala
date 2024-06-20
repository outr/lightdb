package lightdb.document

import lightdb.util.Unique
import lightdb.Id
import lightdb.collection.Collection
import lightdb.index.Index
import lightdb.store.Store

import java.util.concurrent.atomic.AtomicBoolean

trait DocumentModel[D <: Document[D]] { model =>
  type I[F] = Index[F, D]

  private[lightdb] val _initialized = new AtomicBoolean(false)
  private[lightdb] var collection: Collection[D, _] = _
  private[lightdb] var store: Store[D] = _

  final def initialized: Boolean = _initialized.get()

  def parallel: Boolean = true

  object listener {
    private var list = List.empty[DocumentListener[D]]

    def +=(listener: DocumentListener[D]): Unit = synchronized {
      if (initialized) throw new RuntimeException("Already initialized. Listeners should be added before initialization")
      list = (list ::: List(listener)).sortBy(_.priority)
    }

    def -=(listener: DocumentListener[D]): Unit = synchronized {
      list = list.filterNot(_ eq listener)
    }

    def apply(): List[DocumentListener[D]] = list
  }

  def id(value: String = Unique()): Id[D] = Id(value)
}