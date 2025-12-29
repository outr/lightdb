package lightdb.doc

import lightdb.field.Field
import lightdb.filter.{Filter, ParentChildRelation}
import lightdb.id.Id
import lightdb.store.Collection

/**
 * Mix-in trait to add parent-child support for filtering. This should be applied to the Parent model referencing the
 * child.
 *
 * @tparam Doc the Doc
 * @tparam Child the Child document type
 * @tparam ChildModel the Child's Model
 */
trait ParentChildSupport[Doc <: Document[Doc], Child <: Document[Child], ChildModel <: DocumentModel[Child]] extends DocumentModel[Doc] {
  def childStore: Collection[Child, ChildModel]

  def parentField(childModel: ChildModel): Field[Child, Id[Doc]]

  /**
   * Default join-domain name for backends that support a shared physical join-domain index (e.g. OpenSearch).
   *
   * This is intentionally expressed in terms of the resolved parent store name so applications that use custom store
   * names can keep join-domain naming stable.
   */
  def joinDomainName(parentStoreName: String): String = parentStoreName

  /**
   * Name of the join field used by backends that support a physical join-domain.
   */
  def joinFieldName: String = "__lightdb_join"

  /**
   * Default child store name for join-domain helpers. Override if your child store name is customized.
   *
   * Note: this relies on `childStore` being wired, which is typically safe after stores are constructed.
   */
  def childStoreName: String = childStore.name

  /**
   * Default name of the child field containing the parent id (for join routing / join compilation).
   */
  def childJoinParentFieldName: String = parentField(childStore.model).name

  lazy val relation: ParentChildRelation.Aux[Doc, Child, ChildModel] = ParentChildRelation(childStore, parentField)

  /**
   * Builds a parent-side filter that matches when a related child satisfies the provided child filter.
   */
  def childFilter(build: ChildModel => Filter[Child]): Filter[Doc] = Filter.ExistsChild(relation, build)

  /**
   * Same-child semantics: all provided child filters must be satisfied by a single child document.
   *
   * This compiles natively to a single `has_child` with a `bool.must` (OpenSearch), or resolves as one ExistsChild
   * when backends do not support native joins.
   */
  def childFilterSameAll(builds: (ChildModel => Filter[Child])*): Filter[Doc] = {
    Filter.ChildConstraints(relation, Filter.ChildSemantics.SameChildAll, builds.toList)
  }

  /**
   * Collective semantics: each provided child filter must be satisfied by at least one child, but not necessarily the same child.
   *
   * This compiles natively to a parent `bool.must` of multiple `has_child` queries (OpenSearch).
   */
  def childFilterCollectiveAll(builds: (ChildModel => Filter[Child])*): Filter[Doc] = {
    Filter.ChildConstraints(relation, Filter.ChildSemantics.CollectiveAll, builds.toList)
  }
}
