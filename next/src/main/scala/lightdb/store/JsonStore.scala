package lightdb.store

import cats.effect.IO
import fabric.Json
import fabric.rw.RW
import lightdb.Id
import lightdb.document.Document
import lightdb.error.IdNotFoundException
import lightdb.transaction.Transaction

trait JsonStore[D <: Document[D]] extends Store[D] { store =>
  def rw: RW[D]

  protected def json2Serialized(json: Json): IO[Serialized]

  protected def serialized2Json(serialized: Serialized): IO[Json]

  override protected def serialize(doc: D): IO[Serialized] = json2Serialized(rw.read(doc))

  override protected def deserialize(serialized: Serialized): IO[D] = serialized2Json(serialized).map(rw.write)

  object json {
    def apply(id: Id[D])(implicit transaction: Transaction[D]): IO[Json] = get(id)
      .map {
        case Some(json) => json
        case None => throw IdNotFoundException(id)
      }

    def get(id: Id[D])(implicit transaction: Transaction[D]): IO[Option[Json]] = getSerialized(id, transaction)
      .flatMap {
        case Some(serialized) => serialized2Json(serialized).map(Some.apply)
        case None => IO.pure(None)
      }

    def set(id: Id[D], json: Json)(implicit transaction: Transaction[D]): IO[Boolean] = json2Serialized(json)
      .flatMap(serialized => setSerialized(id, serialized, transaction))

    def stream(implicit transaction: Transaction[D]): fs2.Stream[IO, Json] = store
      .idStream
      .evalMap(id => getSerialized(id, transaction))
      .unNone
      .evalMap(serialized2Json)
  }
}
