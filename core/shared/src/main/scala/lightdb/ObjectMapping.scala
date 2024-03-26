package lightdb

import fabric.rw.RW
import lightdb.data.DataManager
import lightdb.field.Field

trait ObjectMapping[D <: Document[D]] { om =>
  type FD[F] = Field[D, F]

  private var _fields: List[Field[D, _]] = Nil

  val _id: Field[D, Id[D]] = field("_id", _._id)

  def fields: List[Field[D, _]] = _fields

  def dataManager: DataManager[D]

  object field {
    def get[F](name: String): Option[FD[F]] = _fields.find(_.name == name).map(_.asInstanceOf[Field[D, F]])

    def apply[F](name: String, getter: D => F)
                (implicit rw: RW[F]): FD[F] = {
      replace(Field[D, F](name, getter, om))
    }

    def replace[F](field: Field[D, F]): FD[F] = om.synchronized {
      _fields = _fields.filterNot(_.name == field.name) ::: List(field)
      field
    }
  }
}