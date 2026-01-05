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
  - `lightdb.opensearch.ignoreMappingHash`: if `true`, skips mapping-hash verification on init (escape hatch for legacy indices)
  - `lightdb.opensearch.useIndexAlias`: if `true`, use a stable alias for the collection and create a physical index on first init (default `false`)
  - `lightdb.opensearch.indexAliasSuffix`: suffix for the first physical index when `useIndexAlias=true` (default `_000001`)
  - `lightdb.opensearch.useWriteAlias`: if `true` (and `useIndexAlias=true`), also create and write through `<alias>_write` (default `false`)
  - `lightdb.opensearch.writeAliasSuffix`: suffix for the write alias (default `_write`)
- **Refresh behavior**
  - `lightdb.opensearch.refreshPolicy`: e.g. `true`, `wait_for`, or omit for default
  - `lightdb.opensearch.<StoreName>.refreshPolicy`: store-specific override
    - Note: `truncate` uses `_delete_by_query`, which only accepts `refresh=true|false`. If configured as `wait_for`, LightDB will normalize it to `true` for truncate.
  - Per-transaction override: for OpenSearch transactions you can override refresh without changing global config:

```scala
db.docs.transaction { tx0 =>
  tx0 match {
    case tx: lightdb.opensearch.OpenSearchTransaction[Doc, Doc.type] =>
      tx.withRefreshPolicy(Some("true")) // or "wait_for" / "false" / None
        .insert(doc)
        .next(tx.commit)
    case _ => Task.error(new RuntimeException("Expected OpenSearchTransaction"))
  }
}
```
- **Auth**
  - `lightdb.opensearch.authHeader`: full `Authorization` header value, e.g. `Bearer ...`
  - or:
    - `lightdb.opensearch.username` + `lightdb.opensearch.password` (Basic)
    - `lightdb.opensearch.bearerToken` (Bearer)
    - `lightdb.opensearch.apiKey` (ApiKey)
- **Timeouts**
  - `lightdb.opensearch.requestTimeoutMillis` (default `10000`)
  - `lightdb.opensearch.<StoreName>.requestTimeoutMillis` (store-specific override)
- **Retries**
  - `lightdb.opensearch.retry.maxAttempts` (default `3`)
  - `lightdb.opensearch.retry.initialDelayMillis` (default `200`)
  - `lightdb.opensearch.retry.maxDelayMillis` (default `5000`)
- **Ingest backpressure**
  - `lightdb.opensearch.ingest.maxConcurrentRequests` (default unlimited) — global limiter shared across stores using the same `baseUrl`
- **Dead-letter capture**
  - `lightdb.opensearch.deadLetter.enabled` (default `false`) — enables best-effort capture of failed bulk items into a dead-letter index
  - `lightdb.opensearch.<StoreName>.deadLetter.enabled` — store-specific override
  - `lightdb.opensearch.deadLetter.indexSuffix` (default `_deadletter`)
- **Bulk sizing**
  - `lightdb.opensearch.bulk.maxDocs` (default `5000`)
  - `lightdb.opensearch.bulk.maxBytes` (default `5242880`)
  - `lightdb.opensearch.bulk.concurrency` (default `1`) — max concurrent `_bulk` requests per commit when chunked
- **Observability**
  - `lightdb.opensearch.opaqueId`: if set, sent as `X-Opaque-Id` on every request (request correlation)
  - `lightdb.opensearch.logRequests`: if `true`, logs request method/path/status/duration and retry attempts (default `false`)
  - `lightdb.opensearch.metrics.enabled`: if `true`, collects in-process metrics (request counts/latency, retries, bulk docs/bytes)
  - `lightdb.opensearch.metrics.logEveryMillis`: if set (and metrics enabled), periodically logs a metrics snapshot
- **Search response shaping**
  - `lightdb.opensearch.search.filterPath`: if set, sent as OpenSearch `filter_path` on `/_search` requests to reduce response payload size
  - `lightdb.opensearch.<StoreName>.search.filterPath`: store-specific override
 - **Facets**
  - `lightdb.opensearch.<StoreName>.facets.includeMissing`: if `true`, include a `$MISSING$` bucket for facet fields with no values (default `false`)

### Cursor pagination (`search_after`)

LightDB’s generic `Query.stream` is offset-based. For scalable deep paging, use the OpenSearch-specific API:

```scala
import lightdb.opensearch.OpenSearchQuerySyntax._

val first = tx.query.cursorPage(pageSize = 100)
val next = first.flatMap(p => tx.query.cursorPage(cursorToken = p.nextCursorToken, pageSize = 100))
```

Note:
- If you configure `lightdb.opensearch.search.filterPath`, cursor pagination requires `hits.hits.sort` to be included so the next cursor token can be generated.
- LightDB will automatically force-include `hits.hits.sort` for cursor pagination requests (even if your configured `filter_path` omits it).

### Join-domain (native `ExistsChild`)

To execute `Filter.ExistsChild` natively (OpenSearch `has_child`), configure a shared join-domain index:

- Parent collection:
  - `lightdb.opensearch.<ParentStoreName>.joinDomain = <domain>`
  - `lightdb.opensearch.<ParentStoreName>.joinRole = parent`
  - `lightdb.opensearch.<ParentStoreName>.joinChildren = ChildStoreName1,ChildStoreName2`
    - Optional for the common single-child case when `<ParentStoreName>` mixes in `ParentChildSupport` (OpenSearch will default `joinChildren` to `childStore.name`).
- Child collection:
  - `lightdb.opensearch.<ChildStoreName>.joinDomain = <domain>`
  - `lightdb.opensearch.<ChildStoreName>.joinRole = child`
  - `lightdb.opensearch.<ChildStoreName>.joinParentField = <childFieldNameContainingParentId>`

This causes both collections to share the same OpenSearch index, and routes children by parent id.

Optional:
- `lightdb.opensearch.<ParentStoreName>.joinScoreMode`: controls OpenSearch `has_child.score_mode` (default `none`; allowed: `none|max|sum|avg|min`)

#### Join-domain config helper (recommended)

If you want to avoid manually specifying the child-side join keys, you can use:

- `lightdb.opensearch.OpenSearchJoinDomainCoordinator.configFor(...)` to **generate** the Profig key/value map
- `lightdb.opensearch.OpenSearchJoinDomainCoordinator.configForDerivedChildStoreName(...)` if your child store name matches the child model name (LightDB default)
- `lightdb.opensearch.OpenSearchJoinDomainCoordinator.configForStoreNames(...)` if you use **custom store names** and want a model-free helper
- `lightdb.opensearch.OpenSearchJoinDomainCoordinator.configForStoreNamesParentOnly(...)` if you want to configure only the **parent** (multi-child map) and let children infer join config
- `lightdb.opensearch.OpenSearchJoinDomainCoordinator.applySysProps(...)` to **apply** it into Profig

Important: apply these **before the stores are created** (e.g., before touching `lazy val` stores), so `OpenSearchConfig.from` sees them.

#### Reduced config option (parent-only Profig keys)

If you prefer configuring only the **parent** and letting the **child** infer its join settings (Profig-driven), set:

- `lightdb.opensearch.<ParentStoreName>.joinDomain = <domain>`
- `lightdb.opensearch.<ParentStoreName>.joinChildren = ChildStoreName1,ChildStoreName2`
- `lightdb.opensearch.<ParentStoreName>.joinChildParentFields = ChildStoreName1:parentIdField,ChildStoreName2:parentIdField`

With these set, the child store can infer `joinDomain`, `joinRole=child`, and `joinParentField` without needing child-side join keys.

#### Programmatic config option (no Profig keys)

If you prefer configuring join-domains in code (without Profig keys), you can register them in-process via:

- `lightdb.opensearch.OpenSearchJoinDomainRegistry.register(dbName, joinDomain, parentStoreName, childJoinParentFields, joinFieldName)`

`OpenSearchConfig.from` will consult this registry when join keys are not present in Profig.

### Parent/child semantics helpers (recommended)

LightDB supports two common semantics when filtering parents by child attributes:

- **Same-child semantics**: all child constraints must match a *single* child (OpenSearch compiles to a single `has_child` with a `bool.must`)
- **Collective semantics**: child constraints may be satisfied by *different* children (OpenSearch compiles to a parent `bool.must` of multiple `has_child` queries)

These are available as helper APIs on parent models that mix in `ParentChildSupport`:

```scala
// same-child: state AND range must be on the same child
tx.query.filter(_.childFilterSameAll(_.state === Some("UT"), _.range === Some("70W")))

// collective: state can be on one child and range can be on another child
tx.query.filter(_.childFilterCollectiveAll(_.state === Some("WY"), _.range === Some("68W")))
```

Internally, these helpers produce an explicit, introspectable filter shape (`Filter.ChildConstraints`) so native backends can compile the semantics directly and non-native backends can expand/resolve safely.

### Optional core join hooks (advanced)

For advanced use-cases, the core planner exposes extension points for join handling:

- `FilterPlanner.ExistsChildResolver`: controls how `ExistsChild` is resolved when native joins are not available.
- `FilterPlanner.JoinMetadataProvider`: allows backends to provide join metadata for compilation/planning without requiring a child-store transaction.

OpenSearch does not require these for native join compilation, but they are useful for custom backends or specialized planning.

### Mapping conventions & gotchas

- **`_id` handling**
  - OpenSearch treats `_id` as metadata; LightDB stores `_id` as the OpenSearch document id (not in `_source`).
  - A stored keyword field (`__lightdb_id`) is also written for stable prefix scans and deterministic sorts.
  - Nested `_id` fields inside objects are **escaped** to avoid OpenSearch special handling and then unescaped on read.

- **Keyword normalization (optional)**
  - By default, non-tokenized string “exact match” behavior is **case-sensitive** (Lucene parity).
  - If you want case-folding + trimming for `.keyword` fields (term/terms/prefix), enable:
    - `lightdb.opensearch.<StoreName>.keyword.normalize = true`
    - or globally: `lightdb.opensearch.keyword.normalize = true`

- **Join-domain truncation**
  - In a join-domain, multiple collections share a single OpenSearch index; `truncate` is scoped to the current collection’s join type (so truncating a join-child won’t wipe parents).

### Rebuild helpers (offline migrations)

If you treat OpenSearch as a derived index, you can use `OpenSearchRebuild` to build a new physical index and then swap aliases.

- **One-phase rebuild**: create new index, bulk index from a source stream, then swap read/write aliases:
  - `OpenSearchRebuild.rebuildAndRepointAliases(...)`
  - `OpenSearchRebuild.rebuildAndRepointAliasesFromStore(...)` (stream from a source-of-truth LightDB store)
  - `OpenSearchRebuild.rebuildAndRepointAliasesFromSources(...)` (index multiple sources into the same target index; join-domain rebuilds)
- **Two-phase rebuild (best-effort under concurrent writes)**: rebuild, switch **write alias** first, run a **catch-up** stream, then swap **read alias**:
  - `OpenSearchRebuild.rebuildSwitchWriteCatchUpSwapRead(...)`
- **Two-phase rebuild with iterative catch-up**: run multiple catch-up batches (paging token) before swapping the read alias:
  - `OpenSearchRebuild.rebuildSwitchWriteCatchUpLoopSwapRead(...)`

These respect the OpenSearch config knobs for bulk sizing, bulk concurrency, and the global ingest limiter.

#### Join-domain rebuild wrapper

If you have a join-domain (parent + child docs in the same OpenSearch index), you can rebuild it from multiple source-of-truth stores and swap the join-domain aliases via:

- `OpenSearchJoinDomainCoordinator.rebuildAndRepointJoinDomainAliasesFromSources(...)`

Notes:
- The iterative catch-up loop API works on Scala 2.13 + Scala 3, but the full integration spec for it lives under `opensearch/src/test/scala-3/` due to a Scala 2.13 compiler `StackOverflowError` triggered by that specific spec shape. Scala 2.13 coverage is provided via a unit test of the paging/token loop logic.

### SplitCollection usage (derived index)

If you want to treat OpenSearch as a **derived search index** while keeping a separate system-of-record store, use LightDB’s `SplitStoreManager`:

```scala
import lightdb.opensearch.OpenSearchStore
import lightdb.store.split.SplitStoreManager
import lightdb.store.hashmap.HashMapStore

override def storeManager = SplitStoreManager(HashMapStore, OpenSearchStore)
```

This routes writes to the storage store and keeps OpenSearch in sync via the SplitCollection transaction wrapper. See `spec.MapAndOpenSearchSplitSpec`.

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
- `OpenSearchIndexMigration.createIndexAndRepointAliases(client, readAlias, writeAliasOpt, indexBody)`
- `OpenSearchIndexMigration.reindexAndRepointAliases(client, readAlias, writeAliasOpt, newIndexBody)` (uses OpenSearch `/_reindex`)

### Tests

The OpenSearch module uses Testcontainers by default (see `spec.OpenSearchTestSupport`).


