package lightdb.query

import fabric.Json
import fabric.rw.{Asable, RW}
import lightdb.doc.{Document, DocumentModel}

/**
 * Result row for [[lightdb.store.Conversion.DocWithInnerHits]].
 *
 * Carries the parent doc plus its highlights and the raw per-relation inner-hit payloads
 * (`(score, highlights, source-json)`). Callers materialize the inner-hits to a typed shape
 * of their choosing via [[innerHitsFor]] — passing the RW for whatever projection fits the
 * backend's stored `_source`. This pattern lets a single result row carry inner-hits from
 * relations with different child types (no shared existential), and avoids the SplitStore
 * pitfall where decoding the full child case class always fails because OpenSearch only
 * stores indexed fields.
 */
case class DocWithInnerHits[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  doc: Doc,
  highlights: Highlights = Highlights.empty,
  innerHits: Map[String, List[RawInnerHit]] = Map.empty
) {
  /**
   * Materialize the inner-hits for the given relation as `InnerHit[V]`, decoding each
   * `_source` JSON via the supplied `RW[V]`. Returns `Nil` when no inner-hits were
   * configured for the relation or when the backend returned no hits for it.
   *
   * `V` should match the fields actually present in the backend's `_source`. For SplitStores
   * (e.g. RocksDB + OpenSearch) the `_source` typically contains only indexed fields, so
   * `V` should be a small projection — e.g. `case class MatchedName(entityName: String)` —
   * not the full child case class.
   */
  def innerHitsFor[V](relationName: String)(implicit rw: RW[V]): List[InnerHit[V]] =
    innerHits.getOrElse(relationName, Nil).map { raw =>
      InnerHit(value = raw.source.as[V], score = raw.score, highlights = raw.highlights)
    }
}

/**
 * Raw inner-hit payload as returned by the backend. Held internally by
 * [[DocWithInnerHits]] so callers can choose their materialization shape via
 * [[DocWithInnerHits.innerHitsFor]]; not part of the public API surface.
 */
final case class RawInnerHit(source: Json,
                             score: Double = 0.0,
                             highlights: Highlights = Highlights.empty)
