### opensearch — plan & checklist

### Status (current)
- **Cross-Scala support**: Scala 2.13 + Scala 3 ✅
- **Lucene parity via abstract specs**: mostly ✅ (client status-code semantics + error handling must be correct for `_backingStore` bootstrap)
- **Prefix-scanning support** (FileStorage + traversal): uses OpenSearch `search_after` ✅
- **Cursor pagination** (OpenSearch-specific): `search_after` cursor tokens ✅
- **Native `ExistsChild`** (OpenSearch join-domain + `has_child`): supported when configured ✅
- **HTTP client**: Spice-based client in `client/HttpOpenSearchClient.scala` ✅ (status/404 handling, retries, bulk error parsing, timeouts, filter_path)
- **Schema evolution**: mapping-hash verification + alias migration helpers + rebuild helpers ✅

### Goals
- **Primary goal**: Provide an OpenSearch-backed LightDB `Collection` implementation that is a **drop-in backend** for `lightdb.Query` and supports **large-scale, low-latency search**.
- **Key feature goal**: Provide **native `Filter.ExistsChild`** execution (no planner materialization) to support performant parent/child query semantics.
- **Operational goal**: Make indexing **reliable** (bulk, retries, backpressure, idempotency) and searchable results **stable** (refresh policy, consistency knobs).

### Compatibility requirements (must hold)
- [x] **Scala 2.13 and Scala 3 must be supported** for the OpenSearch module and any core changes required by it.
- [x] Avoid Scala 3-specific features unless absolutely required (no split source dirs currently needed).

### Non-goals (initially)
- Building a full system-of-record database on top of OpenSearch (OpenSearch should be treated as a **search index** by default).
- Implementing every OpenSearch feature; target only what is needed for LightDB `Query` parity and high-scale search workloads.

---

### Subproject structure
- [x] Create `opensearch/` module wired into `build.sbt`
- [x] `src/main/scala/lightdb/opensearch/` packages (current):
  - [x] `client/` — Spice HTTP client wrapper (`OpenSearchClient(config)`)
  - [x] `query/` — `OpenSearchSearchBuilder` (`Filter`/`Sort`/facets → DSL)
  - [x] `util/` — JSON helpers + cursor encoding
  - [x] top-level — `OpenSearchStore`, `OpenSearchTransaction`, templates, grouping, cursor page types
- [x] `src/test/scala/spec/` — abstract parity specs + OpenSearch-backed concrete specs

---

### Backend requirements / assumptions
- [x] OpenSearch cluster reachable (single-node is fine).
- [x] Index is treated as **eventually consistent** relative to writes unless configured otherwise.
- [x] For join queries, parent + children must be in the **same OpenSearch index** with correct **routing**.

---

### `lightdb.Query` parity checklist (must be supported)

### Query surface
- [x] `filter(...)` with relevant `Filter` variants (including facets + spatial + planner-fallback ExistsChild)
- [x] `sort(...)` with `Sort.IndexOrder`, `Sort.ByField`, `Sort.BestMatch`, `Sort.ByDistance`
- [x] pagination:
  - [x] `offset`/`limit` supported for compatibility (large offsets remain a guardrail concern for production)
  - [x] **cursor pagination** via OpenSearch `search_after` exposed as a store-specific API (`OpenSearchQuerySyntax.cursorPage`)
  - [x] guardrail: enforce `index.max_result_window` and point users to cursor pagination for deep paging
- [x] `scored`, `minDocScore`, `streamWithScore` semantics
- [x] `countTotal(true|false)` behavior
- [x] `facet(...)` / `facets(...)` producing `FacetResult` with correct semantics
- [x] conversions: `docs`, `id`, `value(field)`, `json`, `materialized`, `docAndIndexes`, `converted`
- [x] `Query.delete` and `Query.update` behavior

---

### Phase 0.5 — Core API changes (recommended to enable native joins cleanly)
> This phase is about making `ExistsChild` more “native-backend-friendly” and explicit about the two required semantics,
> while preserving backward compatibility where possible.

### A) Make `ExistsChild` a pure logical operator
- [x] Keep `Filter.ExistsChild` as a join/semi-join operator that **can be executed natively** by backends that support it.
- [x] Planner resolution/materialization is treated as a **fallback strategy** when `supportsNativeExistsChild=false`.
- [x] (Optional) Revisit where `ExistsChild.resolve(...)` lives; moved fallback resolution into `FilterPlanner.DefaultExistsChildResolver` (ExistsChild remains a pure logical operator; method remains as a backwards-compatible delegate).

### B) Add an explicit grouping operator for the two semantics
- [x] Introduce explicit API surface for both semantics (helper methods on `ParentChildSupport`):
  - [x] **Same-child semantics**: `childFilterSameAll(...)` compiles to a single `ExistsChild` with an ANDed child filter.
  - [x] **Collective semantics**: `childFilterCollectiveAll(...)` compiles to AND of multiple `ExistsChild` filters.
- [x] Introduce a dedicated filter shape: `Filter.ChildConstraints` + `Filter.ChildSemantics` (introspectable; avoids relying on composition patterns).
- [x] Ensure this maps cleanly to OpenSearch queries and can still be planned/resolved for non-native backends (expanded via `FilterPlanner` when needed).
- [x] Add unit tests for both semantics independent of any specific backend (`core/src/test/scala/spec/ParentChildSupportSemanticsSpec.scala`).

### C) (Optional) Decouple join metadata from “must query child store”
- [x] Consider a join-metadata abstraction (or extension point) so native backends can compile joins using:
  - [x] index name / join field name / routing rules
  - [x] parent/child “type” labels (join field relations)
  without requiring access to `relation.childStore.transaction` as part of planning (added `FilterPlanner.JoinMetadata` + `JoinMetadataProvider` + `ExistsChildResolver` extension points).

---

### Phase 0 — choose client strategy + module wiring
- [x] Client approach: HTTP client + manual JSON DSL
- [x] Configuration surface (Profig):
  - [x] baseUrl
  - [x] request timeouts
  - [x] refresh policy (per request + defaults)
  - [x] index naming strategy (db name + collection name + join-domain)
  - [x] auth (Authorization header / basic / bearer / api key)
  - [x] retry/backoff policy — delegated to Spice default retry handler (can be revisited for OpenSearch-specific tuning)
  - [x] bulk sizing controls (max docs / max bytes) — enforced in `OpenSearchTransaction` chunking

---

### Phase 0.1 — Client correctness hardening (required for stable specs)
> This is the “make the HTTP client behave like an OpenSearch client” phase. It is required for `_backingStore` bootstrap and for deterministic tests.

### Status-code semantics (must match OpenSearch)
- [x] `indexExists` (HEAD `/<index>`): `200` → true, `404` → false, else error
- [x] `aliasExists` (HEAD `/_alias/<alias>`): `200` → true, `404` → false, else error
- [x] `getDoc` (GET `/<index>/_doc/<id>`): `200` → Some(_source), `404` → None (index/doc missing), else error
- [x] `deleteDoc` (DELETE `/<index>/_doc/<id>`): `200`/`202` → true, `404` → false, else error
- [x] `createIndex` / `indexDoc` / `updateAliases` / `refreshIndex`: treat any 2xx as success; otherwise error with body
- [x] `bulk` (POST `/_bulk`): treat 2xx as “request accepted”, then parse JSON and raise if `errors=true`
- [x] `count/search/deleteByQuery`: treat any 2xx as success (parse body), else error with body

### URL + path correctness (must be explicit per request)
- [x] Ensure every method sets the correct endpoint path (no accidental `PUT /` / `POST /`)

### No blocking calls in client layer
- [x] No `.sync()` in request logging (reading response content remains async)
- [x] Request logging includes: method + path + status + duration

### Retry/backoff (production hardening)
- [x] Use Spice default retry handler (behavior owned by Spice; OpenSearch-specific tuning can be added later if needed)

### Naming clarity (optional but recommended)
- [x] Use `client.HttpOpenSearchClient` as the concrete Spice-backed HTTP wrapper (avoid confusion with “client API” / old trait naming).

---

### Phase 1 — `OpenSearchStore` + basic CRUD + simple search (single collection)

### Store implementation
- [x] Implement `OpenSearchStore[Doc, Model] extends Collection[Doc, Model]`
  - [x] `TX = OpenSearchTransaction[Doc, Model]`
  - [x] initialization: create index if missing with generated mappings
- [x] Implement `OpenSearchTransaction`:
  - [x] `_insert` / `_upsert` buffering + commit (single-doc for small batches, bulk otherwise)
  - [x] `_delete`, `_get`, `_exists`, `truncate`
  - [x] refresh behavior for test parity (configurable via refreshPolicy)

### Search builder (v1)
- [x] `OpenSearchSearchBuilder` mapping:
  - [x] `Filter.MatchNone`
  - [x] `Filter.Equals`, `Filter.In`, numeric `Range*`
  - [x] `Filter.StartsWith`/`EndsWith` (keyword + regexp)
  - [x] tokenized search mapping for `Tokenized` fields
  - [x] `Filter.Multi` (must/should/mustNot + `minShould`)
- [x] Sorting:
  - [x] `Sort.ByField` (keyword/numeric/date)
  - [x] `Sort.BestMatch` mapping to `_score` sort
  - [x] `Sort.IndexOrder` mapping (`_id` asc)
- [x] Totals:
  - [x] `countTotal=false` uses `track_total_hits=false`
  - [x] `countTotal=true` uses `track_total_hits=true` (or bounded threshold if configured via `lightdb.opensearch.trackTotalHitsUpTo`)
- [x] `minDocScore` mapped to `min_score`
- [x] Materialization: `_source` with conversions (`Conversion.*`) implemented

---

### Phase 2 — Mappings & migrations

### Mapping generation
- [x] Define mapping strategy per `Field`:
  - [x] keyword vs text vs numeric vs boolean vs object vs arrays
  - [x] tokenized fields: `text` + `keyword` subfield for exact/sort
  - [x] facets: derived `__facet` keyword fields suitable for terms aggs
  - [x] spatial: derived `__center` `geo_point` fields for distance queries/sorts
- [x] Define normalization rules (case-folding, trimming) consistent with LightDB field semantics:
  - [x] Default: preserve Lucene parity (case-sensitive exact match for non-tokenized strings)
  - [x] Optional: enable trim+lowercase normalizer for `.keyword` via `lightdb.opensearch.<StoreName>.keyword.normalize=true`

### Schema evolution
- [x] Detect incompatible mapping changes (mapping hash stored in index `_meta` and verified on init)
  - [x] strict failure + manual migration instructions (v1; `ignoreMappingHash` escape hatch)
  - [x] automatic “new index + rebuild + alias swap” workflow:
    - [x] helper can create a new physical index and atomically repoint read/write aliases (`OpenSearchIndexMigration.createIndexAndRepointAliases`)
    - [x] helper can reindex from the current read alias into the new physical index and then repoint aliases (`OpenSearchIndexMigration.reindexAndRepointAliases`)
    - [x] rebuild helper can bulk-index from an application-provided source stream, then swap aliases (`OpenSearchRebuild.rebuildAndRepointAliases`)
    - [x] two-phase helper can switch write alias, run a caller-provided catch-up stream, then swap read alias (`OpenSearchRebuild.rebuildSwitchWriteCatchUpSwapRead`)
    - [x] iterative catch-up loop helper (paged batches + max iterations) for coordinated rebuilds (`OpenSearchRebuild.rebuildSwitchWriteCatchUpLoopSwapRead`)
      - Note: the integration spec for this loop lives under `opensearch/src/test/scala-3/` due to a Scala 2.13 compiler stack overflow; Scala 2.13 is covered via a unit test for the paging/token loop.
    - [x] add rebuild helpers that stream from a source-of-truth store (`OpenSearchRebuild.rebuildAndRepointAliasesFromStore`) and from multiple sources (`OpenSearchRebuild.rebuildAndRepointAliasesFromSources`)
- [x] Provide index aliasing scheme:
  - [x] stable alias for reads/writes (opt-in via `lightdb.opensearch.useIndexAlias`)
  - [x] write alias (optional) for zero-downtime migrations (separate read/write aliases)
  - [x] alias swap helper utilities (`OpenSearchIndexMigration`)

---

### Phase 3 — Facets & aggregations
- [x] Implement `Query.facet` / `Query.facets`:
  - [x] terms aggregation for keyword fields
  - [x] hierarchical facets mapping + result formatting (facet-token approach via `<field>__facet`)
  - [x] limits (childrenLimit, dimsLimit) mapped to agg sizes (with safe overfetch + post-filtering)
- [x] Ensure facet semantics match LightDB:
  - [x] facets computed over the filtered result set (single request with `aggregations`)
  - [x] consistent handling of missing/null values (explicit policy):
    - [x] default: missing facet values are excluded (status quo)
    - [x] optional: include missing bucket via `lightdb.opensearch.<StoreName>.facets.includeMissing=true` (adds `$MISSING$`)

---

### Phase 4 — Cursor pagination (required for large datasets)
- [x] Add a cursor API (store-specific extension):
  - [x] encode `search_after` sort values into an opaque token (`OpenSearchCursor`)
  - [x] stable sorting by appending `Sort.IndexOrder` as a tie-breaker when needed
- [x] Store implementation:
  - [x] `OpenSearchQuerySyntax.cursorPage(...)` returns `OpenSearchCursorPage(results, nextCursorToken)`
  - [x] “next page” uses `search_after` (no offset)
- [x] Guardrails for large offsets (runtime enforcement via `maxResultWindow` + guidance to use cursor pagination)

---

### Phase 5 — Native `ExistsChild` (join semantics)

### Join-domain design (core requirement)
- [x] Define a “join domain” abstraction in the OpenSearch module (configuration-driven):
  - [x] One OpenSearch index can contain multiple logical LightDB document types (via shared `joinDomain` index naming)
    - [x] **parent** documents
    - [x] **child** documents
  - [x] All children routed by parent id (required)
  - [x] OpenSearch `join` field mapping (parent/child)
  - [x] Shared index name derived from `joinDomain`
- [x] Join-domain coordinator:
  - [x] Provide helper to generate/apply join config (`OpenSearchJoinDomainCoordinator`)
  - [x] Reduce config duplication: allow child stores to infer join config from a parent-side `joinChildParentFields` map
  - [x] programmatic relation-to-config mapping (no Profig keys required) via `OpenSearchJoinDomainRegistry`
  - [x] Join-domain alias migration wrappers (reindex + atomic alias repoint via `OpenSearchIndexMigration`)
  - [x] join-domain rebuild wrapper (multi-source rebuild + alias repoint) via `OpenSearchJoinDomainCoordinator.rebuildAndRepointJoinDomainAliasesFromSources`
  - [x] status: join-domain coordinator helpers are complete; coordinated rebuild correctness under concurrent writes remains the caller’s responsibility (two-phase/catch-up helpers exist in `OpenSearchRebuild`)

### Query compilation
- [x] `supportsNativeExistsChild=true` for OpenSearch-backed **parent** collections configured as join parents
- [x] `Filter.ExistsChild` → OpenSearch DSL via `has_child`
  - [x] Document-scope: single `has_child` with inner `bool.must` of criteria (already supported by composing child filters)
  - [x] Entity-scope: parent `bool.must` of multiple `has_child` clauses (already expressed as multiple `ExistsChild` filters in parent `Filter.Multi`)
  - [x] scoring modes/boost semantics for joins (auto-upgrade `score_mode` for scored queries; native `boost` on `has_child` without function_score)
- [x] Scoring semantics:
  - [x] choose `score_mode` (none/max/sum/avg/min) and document it (`lightdb.opensearch.<ParentStoreName>.joinScoreMode`)
  - [x] ensure `minDocScore` works predictably (constant-score for filter-only queries + native has_child covered in `OpenSearchNativeExistsChildSpec`)

### Indexing behavior for joins
- [x] Source-of-truth is per-collection writes:
  - [x] parent docs indexed from parent collection writes
  - [x] child docs indexed from child collection writes
- [x] Required behavior implemented:
  - [x] join field is attached on write
  - [x] routing uses parent id
- [x] triggers-based sync / derived-index usage validated (SplitCollection with system-of-record store + OpenSearch search store)
- [x] Support rebuild:
  - [x] full reindex from storage stores into join domain (via `OpenSearchRebuild.rebuildAndRepointAliasesFromSources` + `RebuildSource.fromStore`)
  - [x] partial rebuild helper: delete subset via query + reindex replacement docs (`OpenSearchRebuild.rebuildSubsetInIndex`)
  - [x] join-domain convenience: delete parent+children by parent id(s) (`OpenSearchRebuild.rebuildJoinDomainByParentIds`)

---

### Phase 6 — Performance + reliability hardening
- [x] Bulk indexing:
  - [x] adaptive batching by size and doc count (chunking via `bulkMaxDocs` / `bulkMaxBytes`)
  - [x] bounded concurrency for `_bulk` chunk submission (`bulk.concurrency`, default sequential)
  - [x] global ingest concurrency guardrail across transactions (`ingest.maxConcurrentRequests`)
  - [x] retry on transient failures with jittered backoff (configurable via `retry.*` and `retryStatusCodes`)
  - [x] dead-letter / failure capture (best-effort capture of failed bulk items into a dedicated dead-letter index; transaction still fails)
- [x] Refresh strategy:
  - [x] configurable refresh policy per transaction (via `OpenSearchTransaction.withRefreshPolicy`)
  - [x] default “searchable soon” behavior documented (global `lightdb.opensearch.refreshPolicy` + per-tx override)
- [x] Caching:
  - [x] avoid `track_total_hits=true` unless requested (`Query.countTotal=false` by default + `trackTotalHitsUpTo` bound when enabled)
  - [x] stable sort requirements for `search_after` (cursor paging appends `Sort.IndexOrder` tie-breaker when needed)
- [x] Observability:
  - [x] metrics: request latency, bulk throughput, retry counts (failed docs captured via dead-letter)
  - [x] structured logs including request ids (`X-Opaque-Id`) + retry-aware request logging (opt-in)

---

### Testing plan (must exist before “ready”)
- [x] Abstract parity specs are enabled with OpenSearch-backed concrete specs
- [x] Added OpenSearch-only integration specs:
  - [x] cursor pagination (`OpenSearchCursorPaginationSpec`)
  - [x] native join-domain ExistsChild (`OpenSearchNativeExistsChildSpec`)
- [x] Test harness: Testcontainers OpenSearch
- [x] Deterministic cleanup: per-test indices (via prefix) + truncate patterns

---

### Documentation
- [x] Add README for the module:
  - [x] configuration examples (endpoints/auth/retries/bulk)
  - [x] cursor pagination guidance
  - [x] join-domain setup + routing rules
  - [x] mapping conventions and gotchas (keyword vs text, Json-as-string, spatial __center, `_id` rules)
  - [x] operational notes (refresh, bulk sizing, max_result_window guardrail, alias-based migrations)


