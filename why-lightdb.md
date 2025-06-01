# Why Use LightDB?

LightDB is a Scala-native, embedded database engine that combines the power of key-value stores, full-text search, graph traversal, and SQL querying, all from a unified, type-safe API. Whether you need raw performance or advanced queries, LightDB abstracts the complexity behind a consistent Scala interface.

---

## 💡 What LightDB Provides

### 1. Scala-First Design

LightDB is written in Scala and designed to blend naturally into a Scala codebase. It provides:

- Type-safe, composable APIs
- Immutable data modeling
- Pure-functional interfaces using `Task`, `Stream`, and more
- No external servers or configuration required; import and go

---

### 2. Unified API Across Many Backends

LightDB supports a wide variety of backend engines and abstracts them under a consistent, type-safe API:

#### Key-Value Stores:
- RocksDB
- LMDB
- ChronicleMap
- MapDB
- HaloDB
- Redis (embedded or remote)

#### Search Indexing:
- Lucene (for full-text search, faceting, field-level analyzers, complex querying)

#### SQL Engines:
- SQLite
- H2
- DuckDB
- PostgreSQL

You write the same code regardless of the backend. In most cases, **switching between stores is a single-line code change**, such as replacing:

```scala
val store = RocksDBStore(...)
```

with

```scala
val store = LMDBStore(...)
```

This allows you to benchmark alternatives, adapt to different environments, or combine them via `SplitCollection`, all without modifying your domain logic or rewriting queries.

---

### 3. Access to Store-Specific Features When You Need Them

While LightDB provides a unified abstraction layer across multiple engines, it doesn't block you from going deeper.

When appropriate, store-specific functionality is exposed through the API. For example:

- Run raw SQL queries when using **PostgreSQL**, **SQLite**, **DuckDB**, or **H2**
- Access **Lucene**'s low-level search features, such as custom analyzers, scorers, and direct index access
- Use **RocksDB** or **LMDB**-specific performance tuning features via native options

This gives you the best of both worlds: a clean, consistent interface for everyday use cases and full access to engine-specific power when needed.

---

### 4. SplitCollection: Combine Stores for Performance and Flexibility

LightDB allows you to combine two different storage engines into a single logical collection using a `SplitCollection`. This lets you optimize for **both performance and functionality** by assigning different roles to different backends.

#### Example: Lucene + RocksDB
- **Lucene** provides powerful full-text indexing, scoring, faceting, and complex queries but is slower for single-record lookups.
- **RocksDB**, on the other hand, is blazingly fast for key-based lookups and graph traversals but lacks expressive query capabilities.

With a `SplitCollection`, you can:
- Use **Lucene** for indexing and querying
- Use **RocksDB** for fast document storage and traversal
- Query both through a **unified API**, as if they were a single store

This gives you the **best of both worlds** without requiring manual synchronization of two databases.

---

### 5. Consistent Transaction Support Across All Stores

Every store in LightDB operates within a transactional context, regardless of whether the underlying storage engine supports transactions natively.

This means:
- **Read/write consistency** across all stores
- Clean, atomic `transaction { ... }` blocks
- Nested transactions, rollbacks, and batching
- Uniform behavior, even on stores like MapDB or Redis that don’t natively support transactions

This guarantees **safe and consistent data operations** across a heterogeneous backend environment.

---

### 6. Structured Document Layer

LightDB builds a rich document layer on top of raw storage:

- Strongly typed document definitions
- Indexed fields for filtering and sorting
- Partial updates, projections, and schema versioning
- Efficient serialization (JSON, binary, or custom codecs)

No more encoding blobs by hand or building your own indexing logic.

---

### 7. SQL-like Query DSL in Pure Scala

Write expressive, composable queries with type safety:

- Filters, joins, projections, groupings, pagination
- A native Scala DSL, no fragile SQL strings
- Works with both embedded and SQL-backed stores
- Full composability with compile-time checking

This is like SQL, but **Scala-native, typesafe, and maintainable**.

---

### 8. Lucene-Powered Full-Text Search

Advanced search is built in:

- Tokenized full-text indexing
- Field-specific analyzers
- Faceted filtering and relevance scoring
- Deep integration with LightDB's document model and queries

Unlike using Lucene directly, you don’t need to manage index lifecycles, analyzers, or query parsing.

---

### 9. Flexible Graph Traversal (and Beyond)

LightDB supports relationship traversal across documents, similar to graph databases, but with **more flexibility and composability**:

- Define relationships using standard documents and indexed fields
- Traverse deeply with filtering, directionality, and loop detection
- Combine graph traversal with full-text search, filters, and aggregations
- Works seamlessly with any backend (e.g., RocksDB for traversal, Lucene for indexing)

This isn’t Gremlin or Cypher. LightDB gives you the **core power of graph traversal**, but without forcing you into a graph-only mindset. You can mix relational, graph, and search operations naturally in a single pipeline.

---

### 10. Fully Embedded Architecture

Everything runs inside your application:

- No network overhead or external services
- Zero-config setup for CLI tools, microservices, or desktop apps
- Highly testable with in-memory or mock stores
- Scales from small jobs to multi-threaded, high-throughput systems

You get the power of a full database engine embedded in your Scala codebase.

---

## 🔍 Why Not Just Use RocksDB, Lucene, or PostgreSQL?

You *could* stitch together raw storage engines, but you'd need to:

- Write custom serialization and indexing logic
- Manage synchronization between stores
- Implement a query engine, DSL, and traversal system
- Handle transactions manually or not at all
- Navigate multiple inconsistent APIs

LightDB handles all of that. You get:

✅ Query expressiveness  
✅ Search and graph support  
✅ Type safety and performance  
✅ Clean abstraction with opt-in low-level power

All in one coherent library.