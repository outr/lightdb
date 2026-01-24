package lightdb.filter

import lightdb.doc.{Document, DocumentModel}
import rapid._

object FilterPlanner {
  /**
   * Join metadata extension point.
   *
   * - Native backends can use this to compile joins without depending on a child store transaction.
   * - The planner's ExistsChild resolution is a fallback strategy; when used, it MAY require a child transaction
   *   depending on the resolver.
   */
  case class JoinMetadata(indexName: Option[String] = None,
                          joinFieldName: Option[String] = None,
                          parentType: Option[String] = None,
                          childType: Option[String] = None,
                          routingHint: Option[String] = None)

  trait JoinMetadataProvider {
    def metadataFor[Parent <: Document[Parent]](relation: ParentChildRelation[Parent]): Option[JoinMetadata]
  }

  trait ExistsChildResolver {
    def resolve[Parent <: Document[Parent]](exists: Filter.ExistsChild[Parent],
                                            parentModel: DocumentModel[Parent],
                                            joinMetadataProvider: Option[JoinMetadataProvider] = None): Task[Filter[Parent]]
  }

  /**
   * Default fallback resolver: materialize parent ids by querying the child store.
   *
   * This is intentionally NOT part of the ExistsChild filter type so ExistsChild remains a pure logical operator.
   */
  object DefaultExistsChildResolver extends ExistsChildResolver {
    override def resolve[Parent <: Document[Parent]](exists: Filter.ExistsChild[Parent],
                                                     parentModel: DocumentModel[Parent],
                                                     joinMetadataProvider: Option[JoinMetadataProvider]): Task[Filter[Parent]] = {
      val parentIdField = parentModel._id.name
      val relation = exists.relation
      relation.childStore.transaction { childTx =>
        val model: exists.ChildModel = childTx.store.model.asInstanceOf[exists.ChildModel]
        val cf = exists.childFilter(model)
        val parentField = relation.parentField(model)

        // Bound parent-id materialization to avoid blowing memory on very large datasets.
        //
        // Default is unbounded to preserve historical behavior. Opt-in with:
        // -Dlightdb.existsChild.maxParentIds=<N>
        // (cap is on DISTINCT parent ids, not raw child rows).
        val maxDistinct: Int = {
          import fabric._
          import fabric.rw._
          import profig.Profig
          Profig("lightdb.existsChild.maxParentIds")
            .get()
            .getOrElse(Null)
            .as[Option[Int]]
            .filter(_ >= 0)
            .getOrElse(Int.MaxValue)
        }

        final class TooManyParents extends RuntimeException("Too many distinct parent ids")
        val seen = scala.collection.mutable.HashSet.empty[lightdb.id.Id[Parent]]

        childTx.query
          .filter(_ => cf)
          .value(_ => parentField)
          .stream
          .evalMap { pid =>
            rapid.Task {
              if pid != null && seen.add(pid) && seen.size > maxDistinct then throw new TooManyParents
            }
          }
          .drain
          .attempt
          .map {
            case scala.util.Success(_) =>
              if seen.isEmpty then Filter.MatchNone[Parent]()
              else Filter.In[Parent, lightdb.id.Id[Parent]](parentIdField, seen.toSeq)
            case scala.util.Failure(_: TooManyParents) =>
              throw new IllegalArgumentException(
                s"ExistsChild resolution exceeded maxParentIds=$maxDistinct distinct parent ids. " +
                  s"Add more selective child filters, redesign to avoid broad ExistsChild, or raise " +
                  s"'lightdb.existsChild.maxParentIds'."
              )
            case scala.util.Failure(t) =>
              throw t
          }
      }
    }
  }

  /**
   * Backwards-compatible entrypoint: resolve with the default ExistsChild resolver.
   */
  def resolve[Doc <: Document[Doc]](filter: Option[Filter[Doc]],
                                    model: DocumentModel[Doc],
                                    resolveExistsChild: Boolean): Task[Option[Filter[Doc]]] =
    resolveWith(filter, model, resolveExistsChild, existsChildResolver = DefaultExistsChildResolver, joinMetadataProvider = None)

  /**
   * Backwards-compatible entrypoint: resolve with the default ExistsChild resolver.
   */
  def resolve[Doc <: Document[Doc]](filter: Filter[Doc],
                                    model: DocumentModel[Doc],
                                    resolveExistsChild: Boolean): Task[Filter[Doc]] =
    resolveWith(filter, model, resolveExistsChild, existsChildResolver = DefaultExistsChildResolver, joinMetadataProvider = None)

  /**
   * Advanced entrypoint allowing custom ExistsChild resolution and optional join metadata provisioning.
   */
  def resolveWith[Doc <: Document[Doc]](filter: Option[Filter[Doc]],
                                        model: DocumentModel[Doc],
                                        resolveExistsChild: Boolean,
                                        existsChildResolver: ExistsChildResolver,
                                        joinMetadataProvider: Option[JoinMetadataProvider]): Task[Option[Filter[Doc]]] =
    filter match {
      case Some(f) => resolveWith(f, model, resolveExistsChild, existsChildResolver, joinMetadataProvider).map(Some(_))
      case None => Task.pure(None)
    }

  /**
   * Advanced entrypoint allowing custom ExistsChild resolution and optional join metadata provisioning.
   */
  def resolveWith[Doc <: Document[Doc]](filter: Filter[Doc],
                                        model: DocumentModel[Doc],
                                        resolveExistsChild: Boolean,
                                        existsChildResolver: ExistsChildResolver,
                                        joinMetadataProvider: Option[JoinMetadataProvider]): Task[Filter[Doc]] = filter match {
    case f: Filter.ChildConstraints[Doc @unchecked] =>
      // Expand the dedicated shape into ExistsChild patterns; then resolve normally based on backend capability.
      resolveWith(f.expandToExistsChildFilters, model, resolveExistsChild, existsChildResolver, joinMetadataProvider)
    case f: Filter.ExistsChild[Doc @unchecked] if resolveExistsChild =>
      existsChildResolver.resolve(f.asInstanceOf[Filter.ExistsChild[Doc]], model, joinMetadataProvider)
    case multi: Filter.Multi[Doc] =>
      multi.filters.map { clause =>
        resolveWith(clause.filter, model, resolveExistsChild, existsChildResolver, joinMetadataProvider)
          .map(resolved => clause.copy(filter = resolved))
      }.tasks.map(resolvedClauses => multi.copy(filters = resolvedClauses))
    case other =>
      Task.pure(other)
  }
}

