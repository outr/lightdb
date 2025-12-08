package lightdb.filter

import lightdb.doc.{Document, DocumentModel}
import rapid._

object FilterPlanner {
  def resolve[Doc <: Document[Doc]](filter: Option[Filter[Doc]],
                                   model: DocumentModel[Doc],
                                   resolveExistsChild: Boolean): Task[Option[Filter[Doc]]] =
    filter match {
      case Some(f) => resolve(f, model, resolveExistsChild).map(Some(_))
      case None => Task.pure(None)
    }

  def resolve[Doc <: Document[Doc]](filter: Filter[Doc],
                                   model: DocumentModel[Doc],
                                   resolveExistsChild: Boolean): Task[Filter[Doc]] = filter match {
    case f: Filter.ExistsChild[Doc @unchecked, _, _] if resolveExistsChild =>
      f.resolve(model)
    case multi: Filter.Multi[Doc] =>
      multi.filters.map { clause =>
        resolve(clause.filter, model, resolveExistsChild).map(resolved => clause.copy(filter = resolved))
      }.tasks.map(resolvedClauses => multi.copy(filters = resolvedClauses))
    case other =>
      Task.pure(other)
  }
}

