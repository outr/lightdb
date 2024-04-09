package lightdb

import cats.effect.IO
import cats.implicits.{catsSyntaxApplicativeByName, toTraverseOps}
import fabric.rw.RW
import lightdb.index._
import lightdb.query.Query

abstract class Collection[D <: Document[D]](val collectionName: String,
                                            protected[lightdb] val db: LightDB,
                                            val autoCommit: Boolean = false) {
  type Field[F] = IndexedField[F, D]

  implicit val rw: RW[D]

  val _id: StringField[D] = index("_id").string(_._id.value, store = true)

  protected lazy val store: Store = db.createStore(collectionName)

  private var _indexedLinks = List.empty[IndexedLinks[_, D]]

  def idStream: fs2.Stream[IO, Id[D]] = store.keyStream

  def stream: fs2.Stream[IO, D] = store.streamJson[D]

  /**
   * Called before set
   */
  protected def preSet(doc: D): IO[D] = IO.pure(doc)

  /**
   * Called after set
   */
  protected def postSet(doc: D): IO[Unit] = for {
    // Update IndexedLinks
    _ <- _indexedLinks.map(_.add(doc)).sequence
    fields = index.fields.flatMap { field =>
      field.createFields(doc)
    }
    _ = index.indexer.addDoc(doc._id, fields)
    _ <- commit().whenA(autoCommit)
  } yield ()

  protected def preDelete(id: Id[D]): IO[Id[D]] = IO.pure(id)

  protected def postDelete(doc: D): IO[Unit] = for {
    // Update IndexedLinks
    _ <- _indexedLinks.map(_.remove(doc)).sequence
    _ <- index.indexer.delete(doc._id)
    _ <- commit().whenA(autoCommit)
  } yield ()

  def set(doc: D): IO[D] = preSet(doc)
    .flatMap(store.putJson(_)(rw))
    .flatMap { doc =>
      postSet(doc).map(_ => doc)
    }
  def modify(id: Id[D])(f: Option[D] => IO[Option[D]]): IO[Option[D]] = get(id).flatMap { option =>
    f(option).flatMap {
      case Some(doc) => set(doc).map(Some.apply)
      case None => IO.pure(None)
    }
  }
  def delete(id: Id[D]): IO[Option[D]] = for {
    modifiedId <- preDelete(id)
    deleted <- get(modifiedId).flatMap {
      case Some(d) => store.delete(id).map(_ => Some(d))
      case None => IO.pure(None)
    }
    _ <- deleted match {
      case Some(doc) => postDelete(doc)
      case None => IO.unit
    }
  } yield deleted
  def truncate(): IO[Unit] = for {
    _ <- store.truncate()
    _ <- _indexedLinks.map(_.store.truncate()).sequence
  } yield ()

  def get(id: Id[D]): IO[Option[D]] = store.getJson(id)
  def apply(id: Id[D]): IO[D] = get(id)
    .map(_.getOrElse(throw new RuntimeException(s"$id not found in $collectionName")))

  /**
   * Creates a key/value stored object with a list of links. This can be incredibly efficient for small lists, but much
   * slower for larger sets of data and a standard index would be preferable.
   */
  def indexedLinks[V](name: String,
                      createKey: V => String,
                      createV: D => V,
                      maxLinks: MaxLinks = MaxLinks.OverflowWarn()): IndexedLinks[V, D] = {
    val il = IndexedLinks[V, D](
      name = name,
      createKey = createKey,
      createV = createV,
      store = db.createStore(s"$collectionName.indexed.$name"),
      collection = this,
      maxLinks = maxLinks
    )
    synchronized {
      _indexedLinks = il :: _indexedLinks
    }
    il
  }

  lazy val query: Query[D] = Query(this)

  object index {
    private var _fields = List.empty[IndexedField[_, D]]

    lazy val indexer: Indexer[D] = Indexer(Collection.this)
    def fields: List[IndexedField[_, D]] = _fields

    def apply(name: String): IndexedFieldBuilder = IndexedFieldBuilder(name)

    protected[lightdb] def register[F](field: IndexedField[F, D]): Unit = synchronized {
      _fields = field :: _fields
    }
  }

  def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return] = index.indexer.withSearchContext(f)

  def size: IO[Int] = store.size

  def commit(): IO[Unit] = index.indexer.commit()

  def dispose(): IO[Unit] = IO.unit

  case class IndexedFieldBuilder(fieldName: String) {
    def tokenized(f: D => String): TokenizedField[D] = TokenizedField(fieldName, Collection.this, f)
    def string(f: D => String, store: Boolean = false): StringField[D] = StringField(fieldName, Collection.this, f, store)
    def int(f: D => Int): IntField[D] = IntField(fieldName, Collection.this, f)
    def long(f: D => Long): LongField[D] = LongField(fieldName, Collection.this, f)
    def float(f: D => Float): FloatField[D] = FloatField(fieldName, Collection.this, f)
    def double(f: D => Double): DoubleField[D] = DoubleField(fieldName, Collection.this, f)
    def bigDecimal(f: D => BigDecimal): BigDecimalField[D] = BigDecimalField(fieldName, Collection.this, f)
  }
}

object Collection {
  def apply[D <: Document[D]](collectionName: String,
                              db: LightDB,
                              autoCommit: Boolean = false)(implicit docRW: RW[D]): Collection[D] =
    new Collection[D](collectionName, db, autoCommit = autoCommit) {
      override implicit val rw: RW[D] = docRW
    }
}