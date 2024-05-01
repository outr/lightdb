package lightdb.model

import cats.effect.IO
import cats.implicits.{catsSyntaxApplicativeByName, toTraverseOps}
import fabric.Json
import fabric.rw.RW
import lightdb.index.IndexedField
import lightdb.{Document, Id, IndexedLinks}

trait DocumentModel[D <: Document[D]] {
  type Field[F] = IndexedField[F, D]

  implicit val rw: RW[D]

  private[lightdb] var _indexedLinks = List.empty[IndexedLinks[_, D]]

  def indexedLinks: List[IndexedLinks[_, D]] = _indexedLinks

  /**
   * Called before preSetJson and before the data is set to the database
   */
  def preSet(doc: D, collection: AbstractCollection[D]): IO[D] = IO.pure(doc)

  /**
   * Called after preSet and before the data is set to the database
   */
  def preSetJson(json: Json, collection: AbstractCollection[D]): IO[Json] = IO.pure(json)

  /**
   * Called after set
   */
  def postSet(doc: D, collection: AbstractCollection[D]): IO[Unit] = for {
    // Update IndexedLinks
    _ <- _indexedLinks.map(_.add(doc)).sequence
    _ <- collection.commit().whenA(collection.autoCommit)
  } yield ()

  def preDelete(id: Id[D], collection: AbstractCollection[D]): IO[Id[D]] = IO.pure(id)

  def postDelete(doc: D, collection: AbstractCollection[D]): IO[Unit] = for {
    // Update IndexedLinks
    _ <- _indexedLinks.map(_.remove(doc)).sequence
    _ <- collection.commit().whenA(collection.autoCommit)
  } yield ()
}
