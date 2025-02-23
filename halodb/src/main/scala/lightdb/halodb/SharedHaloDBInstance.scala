package lightdb.halodb

import fabric._
import lightdb.Id
import rapid.Task

case class SharedHaloDBInstance(direct: DirectHaloDBInstance, prefix: String) extends HaloDBInstance {
  direct.counter.incrementAndGet()

  private def uid[Doc](id: Id[Doc]): Id[Doc] = id.copy(s"$prefix${id.value}")

  private def addPrefix(json: Json): Json = {
    json.merge(obj(
      "_id" -> s"$prefix${json("_id").asString}"
    ))
  }

  private def removePrefix(json: Json): Json = {
    json.merge(obj(
      "_id" -> json("_id").asString.substring(prefix.length)
    ))
  }

  override def put[Doc](id: Id[Doc], json: Json): Task[Unit] = direct.put(uid(id), addPrefix(json))

  override def get[Doc](id: Id[Doc]): Task[Option[Json]] = direct.get(uid(id)).map(_.map(removePrefix))

  override def exists[Doc](id: Id[Doc]): Task[Boolean] = direct.exists(uid(id))

  override def count: Task[Int] = stream.count

  override def delete[Doc](id: Id[Doc]): Task[Unit] = direct.delete(uid(id))

  override def stream: rapid.Stream[Json] = direct.stream.collect {
    case json if json("_id").asString.startsWith(prefix) => removePrefix(json)
  }

  override def truncate(): Task[Int] = stream.evalMap { json =>
    val id = json("_id").asString
    delete(Id(id))
  }.count

  override def dispose(): Task[Unit] = if (direct.counter.decrementAndGet() <= 0) {
    direct.dispose()
  } else {
    Task.unit
  }
}
