### lightdb-opensearch

OpenSearch-backed `Collection` implementation for LightDB (Scala 2.13 + Scala 3).

### Configuration

Required:
- `lightdb.opensearch.baseUrl`: e.g. `http://localhost:9200`

Optional:
- **Index naming**
  - `lightdb.opensearch.indexPrefix`: prefix for all indices (useful for tests)
  - `lightdb.opensearch.maxResultWindow`: defaults to `250000`
  - `lightdb.opensearch.trackTotalHitsUpTo`: if set, bounds `track_total_hits` to avoid expensive exact totals (only applies when `Query.countTotal=true`)
  - `lightdb.opensearch.useIndexAlias`: if `true`, use a stable alias for the collection and create a physical index on first init (default `false`)
  - `lightdb.opensearch.indexAliasSuffix`: suffix for the first physical index when `useIndexAlias=true` (default `_000001`)
  - `lightdb.opensearch.useWriteAlias`: if `true` (and `useIndexAlias=true`), also create and write through `<alias>_write` (default `false`)
  - `lightdb.opensearch.writeAliasSuffix`: suffix for the write alias (default `_write`)
- **Refresh behavior**
  - `lightdb.opensearch.refreshPolicy`: e.g. `true`, `wait_for`, or omit for default
    - Note: `truncate` uses `_delete_by_query`, which only accepts `refresh=true|false`. If configured as `wait_for`, LightDB will normalize it to `true` for truncate.
- **Auth**
  - `lightdb.opensearch.authHeader`: full `Authorization` header value, e.g. `Bearer ...`
  - or:
    - `lightdb.opensearch.username` + `lightdb.opensearch.password` (Basic)
    - `lightdb.opensearch.bearerToken` (Bearer)
    - `lightdb.opensearch.apiKey` (ApiKey)
- **Retries**
  - `lightdb.opensearch.retry.maxAttempts` (default `3`)
  - `lightdb.opensearch.retry.initialDelayMillis` (default `200`)
  - `lightdb.opensearch.retry.maxDelayMillis` (default `5000`)
- **Bulk sizing**
  - `lightdb.opensearch.bulk.maxDocs` (default `5000`)
  - `lightdb.opensearch.bulk.maxBytes` (default `5242880`)
- **Observability**
  - `lightdb.opensearch.opaqueId`: if set, sent as `X-Opaque-Id` on every request (request correlation)
  - `lightdb.opensearch.logRequests`: if `true`, logs request method/path/status/duration and retry attempts (default `false`)

### Cursor pagination (`search_after`)

LightDB’s generic `Query.stream` is offset-based. For scalable deep paging, use the OpenSearch-specific API:

```scala
import lightdb.opensearch.OpenSearchQuerySyntax._

val first = tx.query.cursorPage(pageSize = 100)
val next = first.flatMap(p => tx.query.cursorPage(cursorToken = p.nextCursorToken, pageSize = 100))
```

### Join-domain (native `ExistsChild`)

To execute `Filter.ExistsChild` natively (OpenSearch `has_child`), configure a shared join-domain index:

- Parent collection:
  - `lightdb.opensearch.<ParentStoreName>.joinDomain = <domain>`
  - `lightdb.opensearch.<ParentStoreName>.joinRole = parent`
  - `lightdb.opensearch.<ParentStoreName>.joinChildren = ChildStoreName1,ChildStoreName2`
- Child collection:
  - `lightdb.opensearch.<ChildStoreName>.joinDomain = <domain>`
  - `lightdb.opensearch.<ChildStoreName>.joinRole = child`
  - `lightdb.opensearch.<ChildStoreName>.joinParentField = <childFieldNameContainingParentId>`

This causes both collections to share the same OpenSearch index, and routes children by parent id.

Optional:
- `lightdb.opensearch.<ParentStoreName>.joinScoreMode`: controls OpenSearch `has_child.score_mode` (default `none`; allowed: `none|max|sum|avg|min`)

### Mapping conventions & gotchas

- **`_id` handling**
  - OpenSearch treats `_id` as metadata; LightDB stores `_id` as the OpenSearch document id (not in `_source`).
  - A stored keyword field (`__lightdb_id`) is also written for stable prefix scans and deterministic sorts.
  - Nested `_id` fields inside objects are **escaped** to avoid OpenSearch special handling and then unescaped on read.

- **String fields**
  - Indexed strings are mapped as `text` with a `.keyword` subfield for exact matching/sorting.
  - Sorting uses the `.keyword` subfield automatically.

- **`DefType.Json` fields**
  - Stored as a **compact JSON string** (mapped as `keyword`) to avoid dynamic-mapping conflicts when values vary between object/array/scalar across documents.
  - Read paths that materialize JSON will parse the string back to JSON.

- **Spatial fields**
  - GeoJSON is stored as-is, but distance queries/sorts use a derived `geo_point` field with the `__center` suffix.

- **Facets**
  - Facets use derived `<facetName>__facet` keyword tokens to support hierarchical semantics.

### Index migration / aliasing (recommended for production)

For schema/mapping changes that are not compatible with in-place updates, the recommended workflow is:

- Create a new index (e.g. `myindex_v2`) with the new mappings/settings
- Reindex from the old index (or rebuild from your system of record)
- Swap a stable read alias (and optionally a write alias) to point at the new index

The OpenSearch client exposes minimal alias operations via `/_aliases` to support this workflow.

If you enable `lightdb.opensearch.useIndexAlias=true`, LightDB will:
- create a physical index named `<alias><indexAliasSuffix>` (default: `<alias>_000001`)
- create `<alias>` pointing at that physical index

If you also enable `lightdb.opensearch.useWriteAlias=true`, LightDB will additionally:
- create `<alias><writeAliasSuffix>` (default: `<alias>_write`) with `is_write_index=true`
- route writes/deletes/truncate through the write alias, and reads through the read alias

For application-controlled migrations, the module also provides `OpenSearchIndexMigration`:
- `OpenSearchIndexMigration.repointAlias(client, alias, targetIndex)`
- `OpenSearchIndexMigration.repointReadWriteAliases(client, readAlias, writeAliasOpt, targetIndex)`

### Tests

The OpenSearch module uses Testcontainers by default (see `spec.OpenSearchTestSupport`).


