# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
sbt compile                          # Compile all modules
sbt <module>/compile                 # Compile a single module (e.g. sbt halodb/compile)
sbt test                             # Run all tests (only @EmbeddedTest-tagged tests run)
sbt <module>/test                    # Run tests for one module
sbt "testOnly spec.HaloDBSpec"       # Run a single test spec
sbt "<module>/testOnly spec.FooSpec" # Run a single test spec in a module
```

Tests are filtered to `@EmbeddedTest`-tagged suites only (see `build.sbt` line 58). Tests that require external services (Redis, PostgreSQL, OpenSearch, Google Sheets) skip gracefully when those services are unavailable.

## Architecture

LightDB is a pluggable database framework. Users define `Document`/`DocumentModel` case classes, wire them into a `LightDB` subclass via `store(Model)`, and pick a backend (`StoreManager`).

### Core Type Hierarchy

- **`StoreManager`** — Factory that creates `Store` instances. Each backend module provides one (e.g., `HaloDBStore`, `SQLiteStore`, `LuceneStore`).
- **`Store[Doc, Model]`** — Key-value persistence. Supports CRUD, streaming, truncate. Transaction-based access only.
- **`Collection[Doc, Model] extends Store`** — Adds query support (filter, sort, aggregate, facet). Backends: Lucene, OpenSearch, SQL family.
- **`CollectionManager extends StoreManager`** — Factory for `Collection` instances.
- **`Transaction[Doc, Model]`** — Scoped CRUD handle. Auto-committed on exit.
- **`CollectionTransaction extends Transaction`** — Adds `query` DSL for searchable backends.

### Key-Value Stores vs Collections

**KV-only stores** (implement `Store` + `StoreManager`): HaloDB, RocksDB, MapDB, LMDB, ChronicleMap, Redis, Google Sheets. These only support `_id`-based lookup — `_get` on any non-`_id` index throws `UnsupportedOperationException`.

**Collections** (implement `Collection` + `CollectionManager`): Lucene, OpenSearch, SQLite, PostgreSQL, H2, DuckDB. These add the `Query` DSL with filtering, sorting, aggregation, and faceting.

### Adding a New KV Store Backend

Follow the HaloDB pattern — three files:

1. **`FooInstance`** — API wrapper (handles actual I/O). Methods: `put`, `get`, `exists`, `count`, `stream`, `delete`, `truncate`, `dispose`.
2. **`FooStore`** — `case class extends StoreManager` with `create()` factory. Also contains a private `Store` subclass.
3. **`FooTransaction`** — `case class extends Transaction`, delegates everything to the instance.

### Insert vs Upsert contract

`Transaction.insert(doc)` is strict: if `doc._id` already exists, the call fails with
`lightdb.error.DuplicateIdException`. `Transaction.upsert(doc)` is create-or-replace and never
errors on existing ids — use it for idempotent setup, bulk loads, or anywhere the caller doesn't
want duplicate detection.

Backends honor strict-insert through native primitives where possible (LMDB
`MDB_NOOVERWRITE`, SQL PK constraints, ChronicleMap `putIfAbsent`, OpenSearch `op_type=create`).
Backends without one (RocksDB, HaloDB, MapDB, Redis, GoogleSheets, Tantivy) fall back to an
`_exists`-then-`_upsert` default — adds one extra read per insert. With a buffered/queued/async
write handler, the duplicate is detected at flush time rather than at the call site, so error
locality differs by `BatchConfig`.

Lucene is the one exception: `addDocument` has no native uniqueness primitive and every
existence-probe approach (NRT searcher refresh, `DirectoryReader.open(IndexWriter)`) perturbs
TF/IDF stats or segment composition. `_insert` on Lucene preserves its pre-existing behavior
(silently appends another doc with the same `_id`) — callers needing strict semantics should
use `upsert`, or pair Lucene with a KV storage backend via `SplitStoreManager` (the storage
side enforces uniqueness).

### StoreMode

- `StoreMode.All()` (default) — store holds complete documents.
- `StoreMode.Indexes(storage)` — store only holds indexed fields; documents live in `storage`. Used by `SplitStoreManager` to combine e.g. RocksDB (storage) + Lucene (search).

### Effects Model

Uses the [Rapid](https://github.com/outr/rapid) library (not cats-effect). `Task[A]` is a monadic async computation backed by virtual threads. Key patterns:
- `Task(expr)` — wrap synchronous code
- `Task.defer { ... }` — lazy evaluation
- `.map`, `.flatMap`, `.next` — composition
- `.sync()` — block for result (tests and CLI only)
- `rapid.Stream` — async streaming
- `Initializable`/`Disposable` traits with `.init`/`.dispose` lazy singleton Tasks

### JSON Layer

Uses the [fabric](https://github.com/typelevel/fabric) library. Key types: `Json`, `Obj`, `Str`, `NumInt`, `NumDec`, `Bool`, `Null`, `Arr`. Parsing: `JsonParser(string)`. Formatting: `JsonFormatter.Compact(json)`. Serialization: `doc.json(model.rw)` / `json.as[Doc](model.rw)`.

## Module Layout

```
core/       — Core abstractions (Store, Transaction, Document, Query, Field, Filter)
sql/        — Abstract SQL store + connection pooling (extended by sqlite, h2, postgresql, duckdb)
traversal/  — Graph traversal DSL for prefix-scanning stores
lucene/     — Apache Lucene full-text search
opensearch/ — OpenSearch distributed search
halodb/     — HaloDB embedded KV store
rocksdb/    — RocksDB LSM KV store
mapdb/      — MapDB in-memory/persisted KV
lmdb/       — LMDB memory-mapped KV
chronicleMap/ — Chronicle Map off-heap KV
redis/      — Redis network KV
googleSheets/ — Google Sheets API-backed KV store
all/        — Aggregates all modules
benchmark/  — JMH benchmarks
```

## Test Conventions

- **`@EmbeddedTest`** (Java annotation in `core/src/test/scala/spec/EmbeddedTest.java`) — tags suites to run in CI. Only `@EmbeddedTest` suites execute via `sbt test`.
- **`AbstractKeyValueSpec`** — base for KV store tests (CRUD, streaming, bulk insert). Override `storeManager` and optionally `CreateRecords` and `truncateAfter`.
- **`AbstractBasicSpec`** — base for Collection tests (queries, filters, aggregation, facets).
- Tests requiring external services (OpenSearch, PostgreSQL, Redis, Google Sheets) should skip gracefully when unavailable rather than failing.

## Document/Model Pattern

```scala
case class Person(name: String, age: Int, _id: Id[Person] = Person.id()) extends Document[Person]

object Person extends DocumentModel[Person] with JsonConversion[Person] {
  override implicit val rw: RW[Person] = RW.gen
  val name: I[String] = field.index("name", _.name)  // I = indexed field
  val age: F[Int] = field("age", _.age)               // F = non-indexed field
}
```

- `F[T]` — plain field (stored, not queryable)
- `I[T]` — indexed field (stored and queryable in Collections)
- Field definitions use `field(name, accessor)` or `field.index(name, accessor)` / `field.tokenized(name, accessor)`
