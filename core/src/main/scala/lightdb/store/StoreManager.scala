package lightdb.store

import cats.effect.IO
import fabric.rw.RW
import lightdb.document.Document

import java.util.concurrent.ConcurrentHashMap

trait StoreManager {
  private val map = new ConcurrentHashMap[String, IO[Store[_]]]

  def apply[D <: Document[D]](name: String)(implicit rw: RW[D]): IO[Store[D]] = {
    map.computeIfAbsent(name, _ => {
      create[D](name).memoize.flatten
    }).asInstanceOf[IO[Store[D]]]
  }

  protected def create[D <: Document[D]](name: String)(implicit rw: RW[D]): IO[Store[D]]
}
