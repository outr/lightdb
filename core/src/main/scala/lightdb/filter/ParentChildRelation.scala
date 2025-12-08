package lightdb.filter

import lightdb.doc.{Document, DocumentModel}
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
  Child <: Document[Child]
](
  childStore: Collection[Child, _ <: DocumentModel[Child]],
  parentId: Child => Id[Parent]
)

