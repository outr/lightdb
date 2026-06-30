# LightDB Views (materialized, incrementally maintained)

A **View** is a first-class, queryable collection whose contents are *derived* from other
collections by a relational query, and kept current automatically as the underlying data
changes. Views are backend-agnostic: the query is a relational IR, not SQL, so a view works on
any store backend (SQL backends compile the IR to SQL; other backends interpret it).

## Why

Real materialized views (cross-collection joins, aggregates, anti-joins) cached in the database,
maintained incrementally without a full refresh. Postgres core has no incremental matview
maintenance (only full `REFRESH`); `pg_ivm` is an extension with a limited query subset. This
brings incremental view maintenance (IVM) into LightDB itself, for every backend.

## The key idea: recompute-by-scope, not delta-math

Pure delta maintenance (apply +1/-1 to the view) only works for distributive aggregates and
breaks on anti-joins / MIN-MAX-on-delete / negation. Instead, when a base row changes we
determine which **view partition (scope)** it affects and recompute just that scope. This is
tractable for almost any query shape and cheap when scopes are small and keyed on indexes.

## Architecture

```
            Relation IR  (lightdb.view, core)           ← backend-agnostic, introspectable AST
                 │
     ┌───────────┴────────────┐
 generic engine (default)   SQL-native pushdown (override)
 (RelationEngine: per-Scan   (RelationSql: Rel → one SQL
  Query reads + in-memory     SELECT/JOIN when stores are
  join/filter/agg/union)      co-located in one SQL DB)
                 │
              View[Doc, Model]  (a Collection materialized from a Relation)
                 │
              auto-maintenance runtime (Phase 2)
```

Execution is dispatched per relation: when every dependency shares one `NativeViewExecutor`
(`coLocationKey`) — e.g. all stores in one SQL database — the relation runs **natively** (one SQL
statement, planned with the DB's indexes); otherwise, and for any shape the native executor can't
lower, it runs on the **generic engine** (each `Scan` read through its collection's normal
single-collection `Query`, joins/filters/aggregates/unions combined in Scala). The generic path is
why a view runs on *any* backend, including ones whose stores live in separate databases (e.g. H2
gives each store its own file). Both paths produce identical results — proven by running the same
view suite on H2 (generic) and PostgreSQL (native pushdown, each spec in its own schema).

### 1. Relational IR (`lightdb.view`, core)

A small relational algebra the engine can both *execute* and *introspect*:

- `Relation`: `Scan(store, alias)`, `Filter(src, Cond)`, `Project(src, bindings)`,
  `Join(l, r, JoinType, on)` (Inner/Left), `Aggregate(src, groupBy, bindings)`, `Union(arms)`,
  `Derived(src, alias)` (a sub-relation reused as an aliased join source / derived table).
- `Expr`: `Col(alias, field)` (refactor-safe, built from a `Field`), `Lit`, `Func`, `Cast`,
  `Case`, `Agg` (`Count`/`Max`/`Min`/`Sum`).
- `Cond`: `Cmp` comparisons (`=` `<>` `<` `<=` `>` `>=`), `And`/`Or`/`Not`.

Each node exposes `dependencies: Set[Store[?, ?]]`, so the view's dependency set is introspected
straight from the tree (no hand-declared metadata).

Built with a typed builder using model `Field`s (no stringly columns). Output is bound to the
view's `Model` via `field := expr`, closing the alias↔name seam.

### 2. Generic engine (`RelationEngine`, core)

The default executor. `execute(relation): Task[List[Json]]` reads each `Scan` through its
collection's `jsonStream`, then performs filters / projections / nested-loop inner+left joins /
group-by aggregates (`count`/`max`/`min`/`sum`, conditional via `count(case …)`) / unions /
derived-table joins in memory, returning rows decodable into the view's model. No SQL, so it is
backend-portable.

### 3. View

`View[Doc, Model]` wraps a normal `Collection` (queryable/joinable/indexable like any store) whose
rows come from its `Relation`. `reBuild: Task[Int]` runs the relation through the engine and
replaces the contents (truncate + upsert), returning the row count. `materialization` selects how
it is kept current.

### 4. Auto-maintenance (Phase 2 — built)

Dependencies are derived **from the Relation AST** (`relation.dependencies` = every `Store`
reached by any `Scan`, including inside derived sub-relations), not declared by the developer.

A `Triggered` view subscribes to each dependency via `Store.onCommittedChange`. The critical
timing detail: `StoreTrigger.transactionEnd` fires *before* commit, so recomputing there would read
stale (pre-commit) data. A new `StoreTrigger.transactionCommitted` hook fires *after* a successful
commit, so a rebuild it kicks off observes the committed change. `onCommittedChange` flags the
transaction dirty on insert/upsert/delete (and on truncate) and, post-commit, runs the callback
only if the transaction actually wrote.

Maintenance is **conservative-correct**: each committed change triggers a full `reBuild`. The
rebuilds are serialized and coalescing (a per-view lock + dirty flag), so concurrent dependency
commits never interleave two rebuilds and a burst collapses into the minimum number, while every
committed change is still observed. Initial population (data already present before subscription) is
the caller's responsibility via `reBuild` after `db.init`; triggers keep the view current from then
on.

**Correctness is guaranteed; precision is the tunable.** Full refresh is always correct. A
finer-grained, scope-derived incremental update (propagation keys from the join/group-by/correlation
predicates, recomputing only affected view partitions; e.g. one episode → many profiles) is a later
optimization over the *same* auto-derived dependency set, with full refresh as the conservative
fallback whenever a dependency can't be narrowed.

## Phasing

1. **Relational IR + generic engine + `View.reBuild`** (full materialize). TDD in the sql spec. ✅
2. **Auto-maintenance** (`Triggered`: auto deps, post-commit recompute, serialized+coalescing). TDD:
   a base write auto-updates the view with no manual `reBuild`. ✅
   - *Precision refinement (future):* scope-derived incremental recompute instead of full refresh.
3. **SQL-native pushdown** for co-located SQL stores (`RelationSql` + `SqlViewExecutor`), identical
   results to the generic engine. TDD on real PostgreSQL (per-spec schema). ✅
   - *(future)* a non-SQL native interpreter for other backends that can push down.

## First consumer: WatchSummary (NaboTV)

Per `(profileId, kind, tmdbId)`: movies/other map directly from `WatchState`; shows derive
status via the `tmdb.episodes` anti-join (every `(season, episode)` key resolved = Watched),
faithful and key-based (no count-bug). Replaces a per-search Scala pass with an always-current
cached view.
