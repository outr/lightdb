package lightdb.query

import lightdb.Sort

/**
 * Per-relation configuration for surfacing matched child (or parent) documents alongside their
 * outer hit.
 *
 * Modeled cross-backend so the API stays portable, but only OpenSearch implements it natively
 * (via `inner_hits` on `has_child` / `has_parent`). Other backends accept the spec without error
 * and yield empty inner-hit lists in the result.
 *
 * @param size            max number of inner hits to return per outer hit (OpenSearch default 3)
 * @param sort            ordering applied to the inner hits within an outer hit
 * @param sourceIncludes  if non-empty, restrict child `_source` to these fields (skip large blobs)
 * @param highlight       optional highlight configuration applied to the inner hits
 * @param name            optional logical name for the inner-hit block; defaults to the relation
 *                        (i.e. the child store's name) so callers can locate it in result maps
 */
case class InnerHitsSpec(size: Int = 3,
                         sort: List[Sort] = Nil,
                         sourceIncludes: List[String] = Nil,
                         highlight: Option[HighlightSpec] = None,
                         name: Option[String] = None)
