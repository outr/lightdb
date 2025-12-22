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

  lazy val relation: ParentChildRelation.Aux[Doc, Child, ChildModel] = ParentChildRelation(childStore, parentField)

  /**
   * Builds a parent-side filter that matches when a related child satisfies the provided child filter.
   */
  def childFilter(build: ChildModel => Filter[Child]): Filter[Doc] = Filter.ExistsChild(relation, build)
}
