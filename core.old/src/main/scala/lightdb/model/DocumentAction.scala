package lightdb.model

sealed trait DocumentAction

object DocumentAction {
  case object PreSet extends DocumentAction

  case object PostSet extends DocumentAction

  case object PreDelete extends DocumentAction

  case object PostDelete extends DocumentAction
}