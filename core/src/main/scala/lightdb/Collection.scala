package lightdb

import cats.effect.IO
import fabric.rw.RW

abstract class Collection[D <: Document[D]](implicit val rw: RW[D]) {
  protected lazy val defaultCollectionName: String = getClass.getSimpleName.replace("$", "")
  protected lazy val store: Store = db.createStore(collectionName)
  protected lazy val indexStore: Store = db.createStore(s"$collectionName.indexes")

  protected def db: LightDB
  protected def collectionName: String = defaultCollectionName

  object triggers {
    lazy val beforeSet: Triggers[BeforeSet[D]] = new Triggers
    lazy val afterSet: Triggers[AfterSet[D]] = new Triggers
    lazy val beforeDelete: Triggers[BeforeDelete[D]] = new Triggers
    lazy val afterDelete: Triggers[AfterDelete[D]] = new Triggers
  }
  // TODO: Triggers
  // TODO: set, modify, delete, get, apply
}

class Triggers[T] {
  private var _list = List.empty[T]
  def list: List[T] = _list

  def add(triggers: T*): Unit = synchronized {
    _list = _list ::: triggers.toList
  }

  def remove(triggers: T*): Unit = synchronized {
    val set = triggers.toSet
    _list = _list.filterNot(t => set.contains(t))
  }

  def +=(trigger: T): Unit = add(trigger)

  def -=(trigger: T): Unit = remove(trigger)
}

trait BeforeSet[D <: Document[D]] {
  def apply(document: D): IO[Option[D]]
}

trait AfterSet[D <: Document[D]] {
  def apply(document: D): IO[Unit]
}

trait BeforeDelete[D <: Document[D]] {
  def apply(id: Id[D]): IO[Option[Id[D]]]
}

trait AfterDelete[D <: Document[D]] {
  def apply(id: Id[D]): IO[Unit]
}