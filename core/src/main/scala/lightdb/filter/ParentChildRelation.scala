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
final case class ParentChildRelation[
  Parent <: Document[Parent],
  Child <: Document[Child],
  ChildModel <: DocumentModel[Child]
](
  childStore: Collection[Child, ChildModel],
  parentField: ChildModel => Field[Child, Id[Parent]]
)

