package lightdb.lucene.blockjoin

/**
 * Internal field names and conventions for Lucene block-join indexing.
 *
 * A "block" is a contiguous set of documents indexed together via IndexWriter.addDocuments(...):
 *   child1, child2, ..., childN, parent
 *
 * Lucene block-join queries rely on this ordering and on being able to identify which docs are parents.
 */
object LuceneBlockJoinFields {
  val TypeField: String = "__lightdb_type"
  val ParentIdField: String = "__lightdb_parentId"
  val NestedPathField: String = "__lightdb_nestedPath"

  val ParentTypeValue: String = "parent"
  val ChildTypeValue: String = "child"
  val NestedChildTypeValue: String = "nestedChild"

  val ParentPrefix: String = "p."
  val ChildPrefix: String = "c."

  def mapParentFieldName(name: String): String =
    if name == "_id" then name else s"$ParentPrefix$name"

  def mapChildFieldName(name: String): String =
    if name == "_id" then name else s"$ChildPrefix$name"
}



