### traversal — plan & checklist

### Goals
- **Primary goal**: Provide a high-performance query engine that runs on top of any backend that can support **prefix/range scanning** (e.g., ordered KV stores), without tying the implementation to a specific database.
- **Compatibility goal**: Be a **drop-in backend** that fully supports **`lightdb.Query`** without changing application code.
- **Secondary goals**:
  - Enable **Scala-controlled traversal pipelines** where each stage can refine a candidate set (filters, joins/semi-joins, “contains”, regex verification, traversing related collections).
  - Keep memory usage **predictable and bounded** via streaming set operations and compact candidate representations (ordered streams, bitmaps).
  - Make it easy to replace “relational join/grouping” needs with **index-driven semi-joins** and **streaming aggregations**.

### Non-goals (initially)
- Rebuilding a fully general search engine in v1 (full regex, phrase queries, sophisticated scoring, etc.) unless/until required.
- Supporting arbitrary unindexed operations at scale without guardrails (e.g., broad unscoped regex over huge corpora).

---

### `lightdb.Query` parity checklist (must be supported)

### Query surface
- [x] `filter(...)` with all `Filter` variants (see `lightdb.filter.Filter`) *(optimized paths still expanding, but semantics supported)*
- [x] `sort(...)` with `Sort.BestMatch`, `Sort.IndexOrder`, `Sort.ByField`, `Sort.ByDistance`
- [x] `offset`, `limit`, `pageSize` pagination
- [x] `scored`, `minDocScore`, and `streamWithScore` semantics
- [x] `countTotal(true|false)` behavior *(exact totals supported; additional fast exact-count accelerators are optional optimizations)*
- [x] `facet(...)` / `facets(...)` producing `FacetResult` with correct semantics
- [x] conversions: `docs`, `id`, `value(field)`, `json`, `materialized`, `docAndIndexes`, `converted`
- [x] `Query.delete` and `Query.update` (via `CollectionTransaction.doDelete/doUpdate`)

### Filter parity matrix (implementation strategy)
- [x] **Equals / In / Range**: index-backed (posting lists / range scans)
- [x] **StartsWith / EndsWith**: index-backed (prefix, and reverse-prefix index for endsWith)
- [x] **Contains**: n-gram postings + verify (Option B) *(persisted n-grams + in-memory cache optional)*
- [x] **Regex**: n-gram prefilter when possible; otherwise verify-only with guardrails (must require scope / caps)
- [x] **Multi (AND/OR/minShould/MustNot)**: semantics supported (verify-first) + bounded persisted/in-memory seeding, streaming driver selection, and refinement (include + exclude)
- [x] **DrillDownFacetFilter**: correct semantics; optimized path optional (bitmap/docvalues later)
- [x] **ExistsChild**: semantics supported via planner resolution by default; optional native traversal paths exist for page-only and bounded full resolution
  - Notes:
    - Default behavior is planner-resolved `ExistsChild` → `In(_id, parents)` (bounded via `lightdb.existsChild.maxParentIds`).
    - Optional traversal-native modes improve performance for specific shapes but do not require app-level query changes.
- [x] **MatchNone**: constant empty result
  - Fast path: traversal seeding returns an **empty candidate set**, avoiding any document scan.
  - Additional fast path: when traversal sees an empty seeded id set during execution it returns immediately (with `total=0` when requested).

---

### Core concept
- Queries become **set algebra over ordered iterators**:
  - `AND` → intersection
  - `OR` / `minShould` → union/threshold
  - `NOT` → difference (requires a well-chosen “base” candidate set)
- Expensive predicates (regex/contains) are applied as:
  - **Prefilter** (token or n-gram postings) → **intersect with current scope** → **verify** on a reduced set.

---

### Backend requirements (current TraversalStore)
- Traversal runs on any LightDB store implementing **`PrefixScanningStore`**.
- Persisted indexing requires an **ordered** backing store and a separate per-collection index keyspace (implemented as a sibling `__tindex` store).

---

### Planning heuristics (current)
- [x] Choose smallest/selective seed clause based on bounded stats (postings-count hints)
  - Current implementation: **priority + size heuristic**. We try seedable clauses in this order:
    - `Equals`, `StartsWith` / `EndsWith`
    - `In`
    - `Contains` / `Regex` (literal substring n-gram prefilter only)
    - ranges (`RangeLong` / `RangeDouble`) last (can be expensive unions)
  - For `Filter.Multi`:
    - Prefer the **smallest MUST/FILTER seed** (short-circuit on definitive empty).
    - For pure SHOULD:
      - `minShould == 0`: **never seed** from SHOULD-only (would drop matches).
      - `minShould == 1`: seed with union **only if all SHOULD clauses are seedable** (to avoid dropping matches).
      - `minShould > 1`: seed with **bounded threshold union** (ids that appear in ≥ `minShould` SHOULD seeds), only if all SHOULD clauses are seedable and the working set stays ≤ `maxSeedSize`; otherwise fall back to scan+verify.
  - Empty seed sets are preserved as “unsatisfiable query” to avoid full scans.
  - When a seed set is found, traversal does an additional **seed refinement** step (best-effort):
    - Intersect additional cheap MUST/FILTER postings (currently `Equals` / `StartsWith` / `EndsWith` / `In`).
    - Subtract cheap MUST_NOT postings (same subset as above).
    - `NotEquals` inside a `Filter.Multi` MUST/FILTER clause is treated as an exclusion refinement when possible (subtract equals-postings from the current seed).
    - If `Filter.Multi` has `minShould > 0` and all cheap SHOULD clauses are seedable, traversal applies a SHOULD threshold pass against the current seed to reduce candidates before doc fetch/verify.
    - This is bounded (skips refinement when postings are unavailable/too-large) and is correctness-safe because it only narrows the seed set.
    - Applies to both **persisted** postings seeding and **in-memory indexCache** seeding (when enabled).
> Note: remaining “true postings algebra” improvements are **optimization targets**. Current traversal already supports correct
> `Filter.Multi` semantics via bounded persisted/in-memory seeding + full predicate verification; further work is about reducing
> scan/verify cost and memory at extreme scale.

---

### Features mapping (target scope)

### Filters
- [x] Equals / In / NotEquals *(correctness supported; optimized paths vary by filter composition)*
- [x] RangeLong / RangeDouble *(correctness supported; persisted + streaming seeds available for common cases)*
- [x] StartsWith / EndsWith *(correctness supported; persisted + streaming seeds available; long-prefix cases are approximate + verify)*
- [x] Contains *(n-grams + verify; guardrails available for broad verify-only cases)*
- [x] Regex *(n-gram prefilter when literal extractable, else verify-only + guardrails)*
- [x] ExistsChild / childFilter *(semantics supported; multiple native optimizations exist; edge-keyspace indexes are a future performance accelerator)*
- [x] Drill-down facets *(correctness supported; bitmap/docvalues acceleration is future)*

### Sorting
- [x] Sort by field *(semantics supported; scalable streaming path is numeric-only + opt-in via persisted ordered postings flags)*
- [x] IndexOrder
- [x] BestMatch:
  - Implemented: deterministic heuristic scoring (token overlap) + `minDocScore` gating
  - Future: BM25-like scoring requires term statistics + norms + TopK algorithms

### Totals / counts
- [x] Provide `countTotal(true|false)` behavior (exact totals when requested; early-termination preserved when safe)
- [x] Exact totals supported (correctness)
  - When `countTotal=true`, traversal returns an **exact** total (scan or exact fast-path) consistent with SQL semantics.
  - When `countTotal=false`, traversal avoids doing extra work to count.
- [~] Acceleration (optional, ongoing)
  - Implemented: several **exact fast totals** using persisted postings counts / bounded intersections (Equals, tokenized Equals, safe In, opt-in exact prefix totals).
  - Future: richer **stats/metadata** (prefix counts, block counts, bitmaps) to accelerate more cases without scanning.

### Early termination (execution shortcut)
- [x] If `countTotal=false`, no facets, no expensive sorts, and a `limit` (or `pageSize`) is provided, traversal will **stop scanning early**
  once it has produced `offset + limit` matches (after applying filters + `minDocScore` if set).
  - Supported sort cases: no sort, or `Sort.IndexOrder` only.

### Facets
- [x] v1: “collect while scanning results” across the matched set (correctness-first; can be expensive at very large scale)
- Future: bitmap/docvalues-based facets for fast cardinalities

---

### API surface (how it should feel)
- [x] Provide an explicit “traversal pipeline” API for advanced usage, alongside compatibility with LightDB `Query`.
  - Implemented as `lightdb.traversal.pipeline` (`Pipeline`, `Stages`, `Accumulators`, `DocPipeline`, `LookupStages`).
  - Pipelines are streaming-first where possible (with explicit notes where stages materialize).
- [x] “Scala code in control” primitives:
  - branch/merge (compose stages, `facet2`, `reduce2`)
  - interleave lookups/traversals (`lookupOpt`, `lookupMany`, `lookupManyField`)
  - add verification predicates (`DocPipeline.match`, stage `filter`)
  - short-circuit early (`limit`, `skip`, page-first sorting via `sortByPage`)

---

### Subproject structure (initial)
- [x] Create `traversal/` module
- [x] `build.sbt` wiring (module, dependencies, test layout)
- Current packages:
  - `lightdb.traversal.store` — `TraversalStore` / `TraversalTransaction` / query engine / persisted index
  - `lightdb.traversal.pipeline` — typed aggregation pipeline (`Pipeline`, `Stages`, `Accumulators`, `LookupStages`)

---

### Test coverage
- Core abstract specs are exercised via RocksDB-backed traversal stores in `rocksdb/src/test/scala/spec/`:
  - `RocksDBTraversalStoreSpecs` (core abstract specs parity)
  - `RocksDBTraversalPersistedStreamingSeedSpec`, `RocksDBTraversalTokenizedSpec` (streaming seed paths)
  - `RocksDBTraversalGroupedAggregateSpec`, `RocksDBTraversalAggregationPipelineSpec` (aggregation/pipeline)
  - `RocksDBTraversal*FastTotal*Spec` (fast-total paths)
  - `RocksDBTraversal*ExistsChild*Spec` (native ExistsChild variants)
- Traversal module keeps traversal-specific abstract specs only:
  - `AbstractTraversalPersistedIndexBuildSpec`
  - `AbstractTraversalDocPipelineSpec`
- [x] IMDB-scale performance spec *(opt-in runs; see `benchmark/src/test/scala/spec/AbstractImdbPerformanceSpec.scala`)*
  - [x] Multi-hop graph-style traversal via `links` collection (synthetic edges)
  - [x] ExistsChild-style semi-join pipeline (child predicate → parent ids → parent filter)
  - [x] Validation assertions (scoping correctness, totals/facet invariants, traversal invariants)
  - [x] Runner script: `scripts/run-traversal-imdb-perf.sh`

### Phase 1 — structured query execution (no text)
- [x] Secondary indexes for equals/in/range/prefix (**bootstrap in-memory cache**, maintained on insert/upsert/delete; rebuilt lazily)
- [~] Stream algebra executors (intersect/union/diff/minShould)
  - Implemented (bounded, page-only): `Filter.Multi` + `Sort.IndexOrder` can intersect multiple docId-ordered postings clauses to reduce doc fetch/verify:
    - `Equals` / tokenized `Equals` (eqo/toko)
    - `StartsWith` / `EndsWith` (swo/ewo)
    - `In` (bounded union of eqo prefixes per term)
    - `RangeLong/RangeDouble` (bounded union of rlo/rdo prefixes)
  - Future: full streaming postings algebra (no per-prefix `takeN`) and broader filter coverage (range/in/minShould).
- [~] Query planner with simple cost heuristics *(implemented for persisted/in-memory seeding; richer cost model is future)*
- [x] Field sorting *(semantics supported; scalable streaming path is numeric-only + opt-in via ordered numeric postings)*
- [x] Distance filter/sort (scan + compute, correctness-first)
- [x] AggregateQuery (global) execution is streaming-first (single-pass reductions); concat/concatDistinct necessarily materialize their output values.

### Phase 2 — relations / ExistsChild replacement
- [x] Parent/child edge indexes (both directions)
  - Implemented via existing persisted index structures (no separate `pc/cp` edge store required):
    - **parent → children**: equality postings on the child’s indexed `parentId` field (`eq/eqo` on `parentId`)
    - **child → parent**: persisted per-doc `ref` mapping (`ti:<childStore>:ref:<parentField>:<childId> -> "<parentId>"`)
- [x] Semi-join execution: child matches → parent IDs → dedupe/group
  - Implemented:
    - planner-resolved ExistsChild (`ExistsChild.resolve` → `In(_id, parents)`)
    - traversal-native **page-only** semi-joins (`existsChild.native`, plus parent-driven)
    - traversal-native **bounded full** resolution (`existsChild.nativeFull`)
    - seedable child-filter acceleration: stream child ids from postings and map via `ref` (avoids child doc loads)
- [~] Streaming group/count operations for join/group-like workloads *(available via AggregateQuery + pipeline; dedicated “join/group planner” remains future)*
  - Current (planner-based): `ExistsChild` is resolved into `In(parentId, ...)` by `Filter.ExistsChild.resolve`.
    This is now **bounded** (opt-in) to prevent accidental massive parent-id materialization:
    `-Dlightdb.existsChild.maxParentIds=<N>`.
  - Current (traversal opt-in): `lightdb.traversal.existsChild.native=true`
    - For **page-only** queries (no totals/facets, trivial sort, limit/pageSize provided) traversal will execute a **streaming semi-join**
      (child matches → distinct parent ids → fetch parents → verify remaining parent filter) and early-terminate once it has the requested page.
      - Optimization: if the child store is traversal-backed and its persisted index is ready, and the child filter is seedable,
        traversal can stream matching child ids from postings and map to parent ids via persisted `ref` mapping (no child doc loads).
    - Supports multiple `ExistsChild` MUST/FILTER clauses by chunking driver parent ids and intersecting with each other clause’s parent-id hits
      (config: `lightdb.traversal.existsChild.probeChunkSize`, default 128).
    - Optional (page-only): `lightdb.traversal.existsChild.parentDriven=true`
      - If the parent-side filter (excluding ExistsChild) produces a small persisted seed, traversal will probe children per parent id
        (bounded by `lightdb.traversal.existsChild.parentDriven.maxParents`, default 1024) instead of scanning broad child filters to discover parents.
    - Otherwise traversal falls back to planner resolution (correctness-first).
  - Current (traversal opt-in, non page-only): `lightdb.traversal.existsChild.nativeFull=true`
    - Traversal resolves `ExistsChild` into a bounded parent-id `IN(_id, ...)` filter by scanning matching children and collecting distinct parent ids.
    - Config: `lightdb.traversal.existsChild.nativeFull.maxParentIds` (default 100000).
    - If the distinct parent-id set would exceed the cap, traversal falls back to planner resolution.
    - Note: enabling `nativeFull` also counts as “supportsNativeExistsChild”, so `Query.prepared` will not auto-resolve ExistsChild via planner.
    - Optimization: traversal can map matching child `_id` -> parent id via persisted per-doc `ref` mapping in the child store index keyspace
      (avoids loading child docs just to read `parentField`).
  - Config note:
    - Traversal uses **Profig** for these traversal-only flags.

### Phase 3 — “contains” (n-grams) + verification
- [x] N-gram index builder *(persisted + in-memory cache optional; current gram size=3)*
- [x] Query operator: grams → intersect → verify substring/regex *(Contains seeding implemented; regex prefilter implemented when a literal can be extracted; otherwise verify-only + guardrails)*
- [x] Guardrails for broad regex/contains *(optional; Profig: `lightdb.traversal.guardrails.verify.requireScope=true` with thresholds and early-termination exceptions)*

### Phase 3.5 — persisted postings (cold-start + rebuild avoidance)
- [x] Persisted postings key layout (`TraversalKeys`) + writer (`TraversalPersistedIndex`)
- [x] Query engine uses persisted postings for seeding/cold-start (skip doc scan when index is ready)
- [x] Store-specific persisted index backing store strategy that does **not** change `db.backingStore` semantics/counts *(dedicated sibling KeyValue store via `TraversalStore.effectiveIndexBacking`)*

### Phase 4 — totals & facets
- [x] Exact totals (correctness) — supported when `countTotal=true`
- [~] Totals acceleration (optional, future)
  - Bitmap/block-count acceleration
  - Stats-based estimates (only if/when we explicitly add an “approximate totals” mode)
- [x] Facet collection strategy (phase 1 scan-based) — implemented
- Future: facet acceleration (phase 2 bitmap/docvalues)
  - Implemented: when `countTotal=true` and `filter=None`, traversal uses `backing.count` for the total and can still early-terminate page production
    (no full scan just to count).
  - Implemented: when `countTotal=true` and traversal has an **exact candidate seed** (e.g., pure `Equals` / `In` / range with no verify-only filters),
    traversal can use `seed.size` as the total and still early-terminate page production.
  - Implemented: when `countTotal=true` and persisted seeding declines materialization (maxSeedSize), traversal can still compute an exact total
    for safe equality cases by counting equality postings (currently only for non-string equals to avoid case-sensitivity overcount).
  - Implemented: tokenized `Equals` fast total for the single-token case by counting `tok` postings (case-insensitive, matches traversal semantics).
  - Implemented: tokenized `Equals` fast total for small multi-token queries by bounded intersection of `tok` postings (configurable caps).
  - Implemented (opt-in): `StartsWith` / `EndsWith` fast totals via prefix postings counts when the prefix is provably exact
    (query length <= `prefixMaxLen`, lowercase) and string normalization is consistent.
    - Config: `lightdb.traversal.persistedIndex.fastTotal.prefix.enabled` (default false)
  - Implemented: `In` fast total via sum of equality postings counts when the field is single-valued (non-array) and values are non-string (disjoint by construction).
  - Implemented: persisted-index *streaming* seed for page-only queries when seed materialization is skipped (`maxSeedSize`)
    - Config: `lightdb.traversal.persistedIndex.streamingSeed.enabled` (default true)
    - Config: `lightdb.traversal.persistedIndex.streamingSeed.maxInTerms` (default 32)
    - Config: `lightdb.traversal.persistedIndex.streamingSeed.maxInPageDocs` (default 10000) — cap for `IN` + `IndexOrder`
    - Config: `lightdb.traversal.persistedIndex.streamingSeed.maxRangePrefixes` (default 256)
    - Config: `lightdb.traversal.persistedIndex.streamingSeed.maxRangePageDocs` (default 10000)
    - Config: `lightdb.traversal.persistedIndex.streamingSeed.indexOrder.rangeOversample` (default 10)
    - Config: `lightdb.traversal.persistedIndex.streamingSeed.indexOrder.prefixOversample` (default 4)
    - Ordering:
      - `Equals`: safe for no-sort and `Sort.IndexOrder` (uses docId-ordered postings keys)
        - Tokenized fields: `Equals(tokenizedField, "a b")` is seeded via per-token postings (`tok`/`toko`) as a **superset**
          (seed by the smallest token postings stream, then verify all tokens). This is treated as **approximate** when bounded and will fall back
          to a scan if it can’t fill the requested page.
      - `StartsWith` / `EndsWith`: safe for no-sort and `Sort.IndexOrder` (uses docId-ordered *truncated valuePrefix* postings keys)
        - Note: when query length > `prefixMaxLen`, postings are a superset and must be verified; IndexOrder streaming seed uses bounded oversampling.
      - `In`:
        - no-sort: streams merged postings (postings key order)
        - `IndexOrder`: bounded materialization of only the first `offset+limit` docIds per term from `eqo` postings, then merge+sort (avoids full scans)
      - `RangeLong` / `RangeDouble`:
        - no-sort: streams merged numeric-prefix postings from `rl` / `rd` using range-to-prefix decomposition (must still verify)
        - `IndexOrder`: supported via docId-ordered numeric postings (`rlo` / `rdo`) using a fixed prefix length (low write amplification)
          - Postings are approximate (coarse prefix buckets) and must be verified; IndexOrder streaming seed uses bounded oversampling.

    - Correctness fallback: for approximate + bounded IndexOrder streaming seeds (range buckets, long prefixes),
      traversal will fall back to a full early-terminated scan if the seeded stream cannot produce `offset+limit` matches.

    - Filter.Multi support: when a query’s filter is a `Filter.Multi` (common when chaining `.filter(...)`), traversal can still
      pick a single MUST/FILTER clause as a streaming seed driver (Equals/In/StartsWith/EndsWith/Range) and then verify the full filter.
      - Driver selection: prefers the smallest postings set when a bounded postings count can be computed, else falls back to a priority order.
      - Tokenized Equals: `Equals(tokenizedField, "a b")` is treated as AND-of-tokens and can be used as a Multi driver by seeding from the
        smallest per-token postings stream (`toko` for `IndexOrder`, `tok` otherwise), then verifying the full filter.
      - Config: `lightdb.traversal.persistedIndex.streamingSeed.multi.driverMaxCount` (default 100000)
      - Config: `lightdb.traversal.persistedIndex.streamingSeed.multi.driverMaxCountPrefixes` (default 64) — caps range prefix fanout for count hints
      - Pure SHOULD: if no MUST/FILTER driver is available, traversal can merge a bounded number of seedable SHOULD clause streams and then verify the full filter.
        - Config: `lightdb.traversal.persistedIndex.streamingSeed.multi.maxShouldDrivers` (default 8)
        - SHOULD driver ordering: prefers smaller estimated postings (bounded counts), then falls back to priority ordering.
        - `Sort.IndexOrder` note: for pure SHOULD, traversal uses a bounded "take per clause + merge/sort" strategy to preserve global id ordering
          (concat would break IndexOrder semantics when the smallest clause has higher docIds).
      - Optional refinement: after selecting a driver, traversal can intersect the driver stream with additional small MUST/FILTER postings sets
        (currently `Equals` / `StartsWith` / `EndsWith`) before doc fetch/verify.
        - Config: `lightdb.traversal.persistedIndex.streamingSeed.multi.refine.enabled` (default true)
        - Config: `lightdb.traversal.persistedIndex.streamingSeed.multi.refine.maxIds` (default 50000)
        - Config: `lightdb.traversal.persistedIndex.streamingSeed.multi.refine.maxClauses` (default 2)
      - Optional exclusion refinement: subtract small MUST_NOT/NotEquals postings sets before doc fetch/verify.
        - Config: `lightdb.traversal.persistedIndex.streamingSeed.multi.refineExclude.enabled` (default true)
        - Safety: exclusion refinement is only applied when postings are **provably exact** (e.g., non-string `Equals/NotEquals`,
          or prefix queries with length \(\le\) `prefixMaxLen`). Approximate postings are never used for exclusions.
    - Covered: single indexed `Equals`, `StartsWith`, `EndsWith`

### Persisted postings — streaming API
- Added a low-level streaming postings helper (`TraversalPersistedIndex.postingsStream`) so query planning / totals / future
  optimizations can avoid `Set` materialization where not required.
- Convenience helpers:
  - `TraversalPersistedIndex.postingsCount`
  - `TraversalPersistedIndex.postingsCountUpTo`
  - `TraversalPersistedIndex.postingsTake`
- Numeric ordered range postings:
  - Config: `lightdb.traversal.persistedIndex.numericOrderedPrefixLen` (default 4) — fixed hex prefix length for `rlo/rdo` keys (lower write amplification)

### Phase 5 — ranking (BestMatch “real”)
- Future: term statistics (df/norms) + BM25-like scoring
- Future: TopK retrieval (WAND / block-max WAND)
- Future: cursor-based pagination for ranked results

### Phase 6 — aggregates
- [x] Global aggregates (min/max/avg/sum/count/countDistinct/concat/concatDistinct) for correctness + `AbstractSpecialCasesSpec`
- [x] Grouping aggregates (emit multiple rows) + aggregate-level filtering/sorting (HAVING + ORDER BY)
  - Note: grouped aggregation requires holding per-group accumulator state in memory (like any hash-based group-by).
  - `AggregateQuery.count` (via `aggregateCount`) counts all rows after HAVING, ignoring pagination (matches SQL semantics).

---

### Typed Aggregation Pipeline (MongoDB-like, Scala-safe)

### Goal
- Provide a **MongoDB-style aggregation pipeline** for traversal, but **Scala type-safe** so pipelines remain understandable as they grow.
- Make pipelines a first-class, composable API for:
  - multi-stage analytics (grouping, window-ish transforms, projections)
  - join-like traversals (`lookup`/semi-join)
  - facets / rollups / summaries
  - post-filtering and post-sorting over aggregated results

### Key design constraints
- **Type-safety**: stage inputs/outputs are typed; invalid stage ordering should fail at compile time (or at least early).
- **Composability**: pipeline stages should be modular and reusable (avoid “stringly typed” field paths).
- **Streaming-first execution**: stages operate on streams/iterators where possible; only materialize when required (e.g., `sort`, `facet`, `lookup` fanout).
- **Backed by traversal primitives**: stages should compile down to prefix scans + postings intersections + incremental reducers.

### High-level API concept (sketch)
- `Pipeline[In, Out]` where:
  - `In` is typically `Doc` (or `(Doc, Score)`), or a projected record type
  - `Out` can be another record type, grouped record, or an aggregated “row”
- Stages compose via `.pipe(...)` / `.andThen(...)` producing a new `Pipeline`.

### Stage catalog (Mongo mapping → Scala typed)
- **Match** (`$match`): `Pipeline[Doc, Doc]`
  - takes `Filter[Doc]` / `Query`-like predicate built from `Field`s
  - should reuse traversal query seeding and verification logic
- **Project** (`$project`): `Pipeline[Doc, R]`
  - typed projection into case class / tuple / `Obj` (for dynamic), but encourage typed outputs
  - uses `Field` getters; optionally includes computed expressions
- **AddFields** (`$addFields`): `Pipeline[R, R2]` or `Pipeline[R, R]` (record extension)
  - computed fields from existing record fields
- **Unwind** (`$unwind`): `Pipeline[R, R2]`
  - explodes a `List[A]` field into multiple rows; preserves parent identity
- **Group** (`$group`): `Pipeline[R, G]`
  - typed grouping key + accumulator set
  - accumulators: `count`, `sum`, `avg`, `min`, `max`, `collect`, `collectDistinct`, `topK`, etc.
  - should support hierarchical grouping (multi-key) and incremental reduce
- **Sort/Skip/Limit** (`$sort/$skip/$limit`): `Pipeline[R, R]`
  - sorting may force materialization; for large sets use external sort or topK
- **Lookup** (`$lookup`): `Pipeline[R, R2]`
  - typed join against another `Collection`
  - variants:
    - **semi-join** (existence) for `ExistsChild`/relationship filtering
    - **lookup many** returning `List[Child]` or projected child rows
  - should compile to: (keys from input) → (posting/range scans in other collection) → merge
- **Facet** (`$facet`): `Pipeline[R, Faceted[...]]`
  - run multiple sub-pipelines over the same input stream (bounded)
  - should share upstream selection and avoid re-scanning by caching docId stream / bitmap
- **Count** (`$count`): `Pipeline[R, CountRow]`
  - optionally supports “estimate” vs “exact”

### Type-safety strategy
- Prefer **typed field references** (`Field[Doc, A]`) and typed expressions (`Expr[A]`).
- Use **stage type parameters** to prevent invalid sequences:
  - e.g. `Group` changes row type; `Match` on `Doc` is different than filtering grouped rows.
- Provide “escape hatches” for dynamic use:
  - `Pipeline[R, fabric.Json]` / `Obj` projections for ad-hoc analytics, but keep typed as primary.

### Execution strategy (traversal-friendly)
- Core execution uses **docId streams** and **row streams**:
  - `Match` narrows docIds using persisted postings / in-memory cache; verifies remaining predicates.
  - `Project/AddFields/Unwind` are streaming transforms.
  - `Group` uses incremental hash-map aggregation; for huge cardinality consider:
    - shard by key prefix, spill-to-disk, or use backing store temporary buckets
  - `Lookup` uses batched key collection + prefix scans/postings on the foreign collection.
  - `Facet` uses a shared upstream materialization (bitmap / cached ids) with sub-pipelines consuming it.

### Relationship to existing LightDB APIs
- **Does not replace** `lightdb.Query` (still required for drop-in backend parity).
- Provides an additional “power tool” for complex analytics/traversal workloads.
- Medium-term: `Query.aggregate(...)` could compile into a pipeline subset:
  - global aggregates are the trivial case
  - grouped aggregates align naturally with `Group`

### Phased implementation plan
- **Phase A (API + correctness)**:
  - implement `Pipeline` + `Match` + `Project` + `Group` + `Sort/Limit/Skip` + `Count`
  - execution correctness using full scan fallback
- **Phase B (index-backed match + semi-join lookup)**:
  - `Match` uses postings/range/ngram candidate seeding
  - `Lookup` adds semi-join execution (child → parent ids) with batching
- **Phase C (facets + multi-pipeline)**:
  - add `Facet` stage and shared upstream materialization/bitmap support
- **Phase D (performance + memory guardrails)**:
  - topK optimization, spill strategies, bitmap thresholds, explicit caps for broad regex/contains

### Status (current)
- [x] Phase A started:
  - [x] Minimal typed pipeline scaffolding: `Pipeline`, `Stage`
  - [x] Core stages: `filter`/`map`, `groupBy`, `count` + basic accumulators (`count`, `sum`, `min`, `max`)
  - [x] Smoke spec: `TraversalAggregationPipelineSpec`
  - [x] Integrate `Match` with `lightdb.filter.Filter` + model fields (Doc-specific via `DocPipeline`)
  - [x] Add `unwind`, `sort`, `limit/skip` stages *(correctness-first materialization in Phase A)*
  - [x] Add `groupBy2` (multiple accumulators) + `facet2` (two sub-pipelines)
  - [x] Add correctness-first `$lookup`-style stages (`LookupStage.lookupOpt/lookupOne`)
  - [x] Specs: `TraversalDocPipelineSpec` covers lookup; `TraversalAggregationPipelineSpec` covers groupBy2/facet2
  - [x] Add `groupBy3` (three accumulators)
  - [x] Add correctness-first one-to-many lookup (`LookupStage.lookupMany/lookupManyField`)
  - [x] Specs: `TraversalAggregationPipelineSpec` covers groupBy3; `TraversalDocPipelineSpec` covers lookupMany

### Scaling note: avoid in-memory as default
- The **in-memory index cache** (`TraversalIndexCache`) is now **opt-in** via:
  - `-Dlightdb.traversal.indexCache=true`
- Default behavior avoids building postings in RAM. For 100M+ scale, the intended “default fast path” is persisted/on-disk postings (Phase 3.5+), not an in-memory cache.

- **Streaming-first execution**:
  - `TraversalQueryEngine.search` no longer materializes full result sets; it scans once and keeps only bounded state:
    - totals counter (if requested)
    - facet counts (if requested)
    - page-window results, or a bounded top-K heap when sorting with limit/offset
  - `DocPipeline.match` no longer forces `tx.stream.toVector` unless `indexCache` is enabled.
  - Pipeline stages now include **streaming implementations** for:
    - `Stage.unwind`
    - `Stage.skip`
    - `Stage.limit`
  - Pipeline stages now include **bounded (pagination-first) sorting**:
    - `Stage.sortByPage(key, offset, limit, descending)` keeps only `offset + limit` rows in memory and emits the page.
  - Pipeline stages now include **streaming groupBy** (bounded by number of groups):
    - `Stage.groupBy`
    - `Stage.groupBy2`
    - `Stage.groupBy3`
  - Pipeline stages now include a **streaming facet-like reducer**:
    - `Stage.facetTop(key, limit)` computes top-N counts in one pass (bounded by number of distinct keys)
    - Use this for facet-style outputs without replaying upstream; use `facet2` when you truly need to run multiple sub-pipelines over the same upstream.
  - Pipeline stages now include **streaming reducers** (single pass):
    - `Stage.reduce(acc)` reduces to one output
    - `Stage.reduce2(acc1, acc2)` reduces to two outputs in one pass (useful as a scalable alternative to `facet2` when both branches are reducers)
    - `Accumulators.facetTop` provides a reusable facet-style accumulator to pair with `reduce2`
  - Lookup stages are now **batched/streaming-friendly**:
    - `LookupStages.lookupOpt` and `LookupStages.lookupMany` buffer only `LookupStages.DefaultLookupChunkSize` upstream rows at a time.
    - This avoids `in.toList` on the full upstream (important for 100M+ scale pipelines).
  - Persisted index **ready semantics** (correctness):
    - Persisted postings are only used for candidate seeding when the index is marked **ready**.
    - Ready is only set automatically for **brand-new empty collections**; existing collections should run a backfill build.
    - `TraversalStore.buildPersistedIndex()` clears and rebuilds postings and then marks ready.
    - Optional init-time backfill (opt-in): `-Dlightdb.traversal.persistedIndex.autobuild=true`
  - Persisted index seeding coverage (ready-only):
    - Equals / In (equality postings)
    - Contains / Regex (ngram postings)
    - StartsWith (bounded prefix postings; max length via `-Dlightdb.traversal.persistedIndex.prefixMaxLen`, default 8)
    - EndsWith (bounded reverse-prefix postings; same max length, implemented as prefixes of the reversed value/query)
    - RangeLong / RangeDouble (numeric prefix-bucket postings)
      - Numeric prefix max length via `-Dlightdb.traversal.persistedIndex.numericPrefixMaxLen`, default 16
      - One-sided ranges (`from` only / `to` only) do **not** seed by default (avoid near-full candidate sets).
        Opt-in with `-Dlightdb.traversal.persistedIndex.rangeOneSided=true`
    - Candidate seeding safety:
  - Persisted index write-path batching (scale):
    - `TraversalTransaction.insert(docs)` batches index writes per chunk inside a single indexBacking transaction.
    - `TraversalTransaction.upsert(docs)` batches deindex+index per chunk inside a single indexBacking transaction.
    - `TraversalTransaction.doUpdate` uses chunked bulk upsert to batch persisted index maintenance.
    - `TraversalTransaction.doDelete` batches persisted index deindex per chunk inside indexBacking transactions.
      - Persisted seed scans are bounded by `-Dlightdb.traversal.persistedIndex.maxSeedSize` (default 100000)
      - If a seed would exceed the limit, traversal falls back to scan+verify (correctness preserved; memory protected)
  - Remaining known **pipeline materializers** (acceptable for small data / tests, but avoid by default at large scale):
    - `Stage.sortBy`: materializes full input; prefer `Stage.sortByPage` for pagination flows
    - `Stage.facet2`: materializes upstream to replay into both branches; prefer `Stage.reduce2` when both branches are reducers

- **Persisted candidate seeding for pipelines**:
  - When `-Dlightdb.traversal.persistedIndex=true` is enabled and postings are ready, `DocPipeline.match` can seed from persisted postings instead of scanning.
  - Postings are stored in a **dedicated KeyValue PrefixScanningStore** per collection (not in `_backingStore`), to avoid polluting backups/counts.

- **Persisted index default**:
  - Persisted indexing is **enabled by default** for traversal stores.
  - Disable with `-Dlightdb.traversal.persistedIndex=false` (useful for debugging or minimal-write scenarios).
  - Writes are **batched per document** (single `upsert` of all postings) and best-effort (failures do not affect correctness, only performance/cold-start).

---

### Risk checklist
- Future considerations:
  - Index write amplification (especially n-grams)
  - Update/delete semantics (tombstones vs rebuild vs per-doc value indexes)
  - Snapshot consistency vs near-real-time refresh model
  - Memory ceilings (bitmap thresholds, dedupe strategies, facet accumulation)
  - Operational tooling (reindex, integrity checks, compaction/merge strategy)

---

### Effort estimates (rough, for “nice-to-haves”)
These are intentionally approximate and assume the current traversal + RocksDB spec baseline.

- **True streaming postings algebra (AND/OR/minShould without list materialization)**: **Medium–Large (3–8 days)**
  - Adds merge/intersection/threshold iterators and threads them through Multi planning; requires careful correctness + perf coverage.
- **Facet acceleration (bitmap/docvalues)**: **Large (1–3 weeks)**
  - Requires new persisted structures, write-path maintenance, and new facet execution paths for large result sets.
- **Ranking “real BestMatch” (df/norm stats + BM25 + TopK/WAND)**: **Very Large (2–6 weeks)**
  - Term stats + norms + scoring + TopK retrieval and pagination semantics; extensive tests/tuning.
- **Operational tooling (reindex/integrity/compaction controls)**: **Medium (3–7 days)**
  - Adds safe rebuild + verification routines and production-grade tooling; mostly engineering + harness.
- **Additional exact-total accelerators (more posting-count fast totals)**: **Small–Medium (1–4 days)**
  - Extends safe shapes (more Multi combos / token cases / In+range variants) plus tests.
- **Optional spill-to-disk for high-cardinality grouping**: **Large (1–2+ weeks)**
  - External grouping/spill format + merge strategy; many edge cases and deterministic semantics requirements.


