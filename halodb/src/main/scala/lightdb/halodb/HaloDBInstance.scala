package lightdb.halodb

import fabric.Json
import lightdb.id.Id
import rapid.Task

trait HaloDBInstance {
  def put[Doc](id: Id[Doc], json: Json): Task[Unit]

  def get[Doc](id: Id[Doc]): Task[Option[Json]]

  def exists[Doc](id: Id[Doc]): Task[Boolean]

  def count: Task[Int]

  def stream: rapid.Stream[Json]

  def delete[Doc](id: Id[Doc]): Task[Unit]

  def truncate(): Task[Int]

  def dispose(): Task[Unit]
}
