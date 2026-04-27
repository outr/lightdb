package lightdb.query

import lightdb.doc.{Document, DocumentModel}

/**
 * Result row for [[lightdb.store.Conversion.DocWithInnerHits]].
 *
 * Carries the parent doc plus its highlights and any inner-hit documents from configured
 * relations (typically `has_child` / `has_parent` joins on OpenSearch).
 *
 * Inner-hits are stored as `InnerHit[Any]` since a single query can join multiple relations
 * with different child types — the F-bounded `Document[Doc <: Document[Doc]]` rules out a
 * single existential. Use [[innerHitsFor]] at the call site, where you know the relation's
 * child type, to recover typed access.
 */
case class DocWithInnerHits[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  doc: Doc,
  highlights: Highlights = Highlights.empty,
  innerHits: Map[String, List[InnerHit[Any]]] = Map.empty
) {
  /**
   * Inner hits for the given relation (keyed by child store name), typed as `ChildDoc`.
   *
   * Returns `Nil` when no inner-hits were configured for that relation, or when the backend
   * (anything other than OpenSearch) silently dropped the request.
   */
  def innerHitsFor[ChildDoc <: Document[ChildDoc]](relationName: String): List[InnerHit[ChildDoc]] =
    innerHits.getOrElse(relationName, Nil).asInstanceOf[List[InnerHit[ChildDoc]]]
}
