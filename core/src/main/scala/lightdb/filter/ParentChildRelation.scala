package lightdb.filter

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.store.Collection

/**
 * Describes how a parent document relates to its children.
 *
 * @param childStore store containing child documents
 * @param parentId   function to map a child document to its parent id
 */
trait ParentChildRelation[Parent <: Document[Parent]] {
  type Child <: Document[Child]
  type ChildModel <: DocumentModel[Child]

  def childStore: Collection[Child, ChildModel]
  def parentField(childModel: ChildModel): Field[Child, Id[Parent]]
}

object ParentChildRelation {
  type Aux[
    Parent <: Document[Parent],
    Child0 <: Document[Child0],
    ChildModel0 <: DocumentModel[Child0]
  ] = ParentChildRelation[Parent] {
    type Child = Child0
    type ChildModel = ChildModel0
  }

  def apply[
  Parent <: Document[Parent],
    Child0 <: Document[Child0],
    ChildModel0 <: DocumentModel[Child0]
](
    childStore0: Collection[Child0, ChildModel0],
    parentField0: ChildModel0 => Field[Child0, Id[Parent]]
  ): Aux[Parent, Child0, ChildModel0] = new ParentChildRelation[Parent] {
    override type Child = Child0
    override type ChildModel = ChildModel0
    override val childStore: Collection[Child0, ChildModel0] = childStore0
    override def parentField(childModel: ChildModel0): Field[Child0, Id[Parent]] = parentField0(childModel)
  }
}

