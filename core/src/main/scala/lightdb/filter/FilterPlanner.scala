package lightdb.filter

import lightdb.doc.{Document, DocumentModel}
import rapid._

object FilterPlanner {
  def resolve[Doc <: Document[Doc]](filter: Option[Filter[Doc]], model: DocumentModel[Doc]): Task[Option[Filter[Doc]]] =
    filter match {
      case Some(f) => resolve(f, model).map(Some(_))
      case None => Task.pure(None)
    }

  def resolve[Doc <: Document[Doc]](filter: Filter[Doc], model: DocumentModel[Doc]): Task[Filter[Doc]] = filter match {
    case f: Filter.ExistsChild[Doc @unchecked, _] =>
      f.resolve(model)
    case multi: Filter.Multi[Doc] =>
      multi.filters.map { clause =>
        resolve(clause.filter, model).map(resolved => clause.copy(filter = resolved))
      }.tasks.map(resolvedClauses => multi.copy(filters = resolvedClauses))
    case other =>
      Task.pure(other)
  }
}

