# lightdb
[![CI](https://github.com/outr/lightdb/actions/workflows/ci.yml/badge.svg)](https://github.com/outr/lightdb/actions/workflows/ci.yml)

Computationally focused database using pluggable stores

## Provided Stores
| Store                                                                 | Type               | Embedded | Persistence | Read Perf | Write Perf | Concurrency | Transactions    | Full-Text Search | Prefix Scan | Notes                                 |
|------------------------------------------------------------------------|--------------------|----------|-------------|-----------|------------|-------------|------------------|------------------|-----------|---------------------------------------|
| [HaloDB](https://github.com/yahoo/HaloDB)                              | KV Store           | ✅       | ✅          | ✅        | ✅✅       | 🟡 (Single-threaded write) | 🟡 (Basic durability) | ❌               | ❌        | Fast, simple write-optimized store    |
| [ChronicleMap](https://github.com/OpenHFT/Chronicle-Map)              | Off-Heap Map       | ✅       | ✅ (Memory-mapped) | ✅✅     | ✅✅       | ✅✅         | ❌              | ❌               | ❌        | Ultra low-latency, off-heap storage   |
| [LMDB](https://www.symas.com/mdb)                                      | KV Store (B+Tree)  | ✅       | ✅          | ✅✅✅     | ✅        | 🟡 (Single write txn) | ✅✅ (ACID)     | ❌               | ✅        | Read-optimized, mature B+Tree engine  |
| [MapDB (B-Tree)](https://mapdb.org)                                   | Java Collections   | ✅       | ✅          | ✅        | ✅        | ✅           | ✅              | ❌               | ✅        | Uses BTreeMap for ordered/prefix scans|
| [RocksDB](https://rocksdb.org)                                        | LSM KV Store       | ✅       | ✅          | ✅✅      | ✅✅✅     | ✅           | ✅              | ❌               | ✅        | High-performance LSM tree             |
| [Redis](https://redis.io)                                             | In-Memory KV Store | 🟡 (Optional) | ✅ (RDB/AOF) | ✅✅✅     | ✅✅       | ✅           | ✅              | ❌               | ❌        | Popular in-memory data structure store|
| [Lucene](https://lucene.apache.org)                                   | Full-Text Search   | ✅       | ✅          | ✅✅      | ✅        | ✅           | ❌              | ✅✅✅           | ❌        | Best-in-class full-text search engine |
| [OpenSearch](https://opensearch.org)                                  | Search Server      | ❌ (Server-based) | ✅   | ✅✅✅     | ✅✅      | ✅✅         | 🟡 (Transactional batching; not ACID) | ✅✅✅           | ❌        | Distributed search, joins, aggregations |
| [SQLite](https://www.sqlite.org)                                      | Relational DB      | ✅       | ✅          | ✅        | ✅        | 🟡 (Write lock) | ✅✅ (ACID)     | ✅ (FTS5)         | 🟡        | Lightweight embedded SQL              |
| [H2](https://h2database.com)                                          | Relational DB      | ✅       | ✅          | ✅        | ✅        | ✅           | ✅✅ (ACID)     | ❌ (Basic LIKE)    | 🟡         | Java-native SQL engine                |
| [DuckDB](https://duckdb.org)                                          | Analytical SQL     | ✅       | ✅          | ✅✅✅     | ✅        | ✅           | ✅              | ❌               | 🟡        | Columnar, ideal for analytics         |
| [PostgreSQL](https://www.postgresql.org)                              | Relational DB      | ❌ (Server-based) | ✅   | ✅✅✅     | ✅✅      | ✅✅         | ✅✅✅ (ACID, MVCC) | ✅✅ (TSVector)  | 🟡         | Full-featured RDBMS                   |

### Legend
- ✅: Supported / Good
- ✅✅: Strong
- ✅✅✅: Best-in-class
- 🟡: Limited or trade-offs
- ❌: Not supported

## In-Progress
- Tantivy (https://github.com/quickwit-oss/tantivy) - Working on creating a wrapper around Rust's extremely fast alternative to Apache Lucene (See https://github.com/outr/scantivy)

## SBT Configuration

To add all modules:
```scala
libraryDependencies += "com.outr" %% "lightdb-all" % "4.14.0"
```

For a specific implementation like Lucene:
```scala
libraryDependencies += "com.outr" %% "lightdb-lucene" % "4.14.0"
```

For graph traversal utilities:
```scala
libraryDependencies += "com.outr" %% "lightdb-traversal" % "4.14.0"
```

For OpenSearch:
```scala
libraryDependencies += "com.outr" %% "lightdb-opensearch" % "4.14.0"
```

---

## Recent Additions

### Traversal module (`lightdb-traversal`)
LightDB now includes a lightweight graph traversal DSL that works against any `PrefixScanningTransaction` (e.g. RocksDB / LMDB / MapDB B-Tree / traversal stores).

- **Import**: `import lightdb.traversal.syntax._`
- **Common helpers**:
  - `tx.traverse.edgesFor[Edge, From, To](fromId)`
  - `tx.traverse.reachableFrom[Edge, Node, Node](startId)`
  - `tx.traverse.shortestPaths[Edge, From, To](fromId, toId)`
  - `tx.traverse.bfs(...)` / `tx.traverse.dfs(...)`

Example:
```scala
import lightdb.traversal.syntax._

db.flights.transaction { tx =>
  val lax = Airport.id("LAX")
  tx.storage.traverse
    .reachableFrom[Flight, Airport, Airport](lax)
    .map(_._to)
    .distinct
    .toList
}
```

### `Query.distinct(field)`
LightDB now exposes a backend-agnostic `distinct` API:
```scala
db.people.transaction { tx =>
  tx.query.distinct(_.city).toList
}
// res0: Task[List[Option[City]]] = FlatMap(
//   input = FlatMap(
//     input = Suspend(
//       f = lightdb.store.Store$transaction$$$Lambda/0x0000000014652408@41c10fb,
//       trace = SourcecodeTrace(
//         file = File(
//           "/home/mhicks/projects/open/lightdb/core/src/main/scala/lightdb/store/Store.scala"
//         ),
//         line = Line(131),
//         enclosing = Enclosing("lightdb.store.Store#transaction.create")
//       )
//     ),
//     f = lightdb.store.Store$transaction$$$Lambda/0x0000000014656000@7435ada6,
//     trace = SourcecodeTrace(
//       file = File(
//         "/home/mhicks/projects/open/lightdb/core/src/main/scala/lightdb/store/Store.scala"
//       ),
//       line = Line(136),
//       enclosing = Enclosing("lightdb.store.Store#transaction.create")
//     )
//   ),
//   f = lightdb.store.Store$transaction$$$Lambda/0x0000000014656bb0@2332ad39,
//   trace = SourcecodeTrace(
//     file = File(
//       "/home/mhicks/projects/open/lightdb/core/src/main/scala/lightdb/store/Store.scala"
//     ),
//     line = Line(107),
//     enclosing = Enclosing("lightdb.store.Store#transaction.apply")
//   )
// )
```

Supported backends:
- **OpenSearch**: composite aggregations (paged)
- **Lucene**: grouping (docvalues-based) for scalar fields (String/Enum/Int/Double and Option variants)
- **SQL** (SQLite/H2/DuckDB/PostgreSQL): `SELECT DISTINCT ...` with paging
- **SplitCollection**: delegates to the searching side (e.g. RocksDB + OpenSearch)

---

## OpenSearch Notes (LightDB backend)
OpenSearch support has been expanded to better support large-scale ingestion and production usage.

### Fast ingestion defaults (transactional)
When using OpenSearch as a searching backend, LightDB favors **fast ingestion** over read-your-writes mid-transaction, and forces visibility at commit.

### Facet childCount mode (speed vs exactness)
OpenSearch cannot return an “exact distinct bucket count” for `terms` aggregations without paging.

- Default is **fast/approximate** via `cardinality`.
- You can opt into **exact** via composite paging.

Config:
```json
{
  "lightdb": {
    "opensearch": {
      "facetChildCount": {
        "mode": "cardinality",
        "precisionThreshold": 40000
      }
    }
  }
}
```

### Index sorting (OpenSearch)
LightDB can emit OpenSearch index sorting settings at index creation time:
```json
{
  "lightdb": {
    "opensearch": {
      "index": {
        "sort": {
          "fields": ["unifiedEntityId.keyword", "__lightdb_id"],
          "orders": ["asc", "asc"]
        }
      }
    }
  }
}
```
Note: index sorting requires a new index (it cannot be added to an existing index).

### Truncate behavior
On OpenSearch stores, `truncate` is implemented as **drop + recreate index**, which is dramatically faster than `_delete_by_query` for large indices.

## Videos
Watch this [Java User Group demonstration of LightDB](https://www.youtube.com/live/E_5fwgbF4rc?si=cxyb0Br3oCEQInTW)

## Getting Started

This guide will walk you through setting up and using **LightDB**, a high-performance computational database. We'll use a sample application to explore its key features.

*NOTE*: This project uses Rapid (https://github.com/outr/rapid) for effects. It's somewhat similar to cats-effect, but
with a focus on virtual threads and simplicity. In a normal project, you likely wouldn't be using `.sync()` to invoke
each task, but for the purposes of this documentation, this is used to make the code execute blocking.

---

## Prerequisites

Ensure you have the following:

- **Scala** installed
- **SBT** (Scala Build Tool) installed

---

## Setup

### Add LightDB to Your Project

Add the following dependency to your `build.sbt` file:

```scala
libraryDependencies += "com.outr" %% "lightdb-all" % "4.14.0"
```

---

## Example: Defining Models and Collections

### Step 1: Define Your Models

LightDB uses **Document** and **DocumentModel** for schema definitions. Here's an example of defining a `Person` and `City`:

```scala
import lightdb._
import lightdb.id._
import lightdb.store._
import lightdb.doc._
import fabric.rw._

case class Person(
  name: String,
  age: Int,
  city: Option[City] = None,
  nicknames: Set[String] = Set.empty,
  friends: List[Id[Person]] = Nil,
  _id: Id[Person] = Person.id()
) extends Document[Person]

object Person extends DocumentModel[Person] with JsonConversion[Person] {
  override implicit val rw: RW[Person] = RW.gen

  val name: I[String] = field.index("name", _.name)
  val age: I[Int] = field.index("age", _.age)
  val city: I[Option[City]] = field.index("city", _.city)
  val nicknames: I[Set[String]] = field.index("nicknames", _.nicknames)
  val friends: I[List[Id[Person]]] = field.index("friends", _.friends)
}
```

```scala
case class City(name: String)

object City {
  implicit val rw: RW[City] = RW.gen
}
```

### Step 2: Create the Database Class

Define the database with stores for each model:

```scala
import lightdb.sql._
import lightdb.store._
import lightdb.upgrade._
import java.nio.file.Path

object db extends LightDB {
  override type SM = CollectionManager
  override val storeManager: CollectionManager = SQLiteStore
   
  lazy val directory: Option[Path] = Some(Path.of(s"docs/db/example"))
   
  lazy val people: Collection[Person, Person.type] = store(Person)

  override def upgrades: List[DatabaseUpgrade] = Nil
}
```

---

## Using the Database

### Step 1: Initialize the Database

Initialize the database:

```scala
db.init.sync()
```

### Step 2: Insert Data

Add records to the database:

```scala
val adam = Person(name = "Adam", age = 21)
// adam: Person = Person(
//   name = "Adam",
//   age = 21,
//   city = None,
//   nicknames = Set(),
//   friends = List(),
//   _id = StringId("AAGmPac35fkhX4pwacUuJ6lk0syGxFvk")
// )
db.people.transaction { implicit txn =>
  txn.insert(adam)
}.sync()
// res2: Person = Person(
//   name = "Adam",
//   age = 21,
//   city = None,
//   nicknames = Set(),
//   friends = List(),
//   _id = StringId("AAGmPac35fkhX4pwacUuJ6lk0syGxFvk")
// )
```

### Step 3: Query Data

Retrieve records using filters:

```scala
db.people.transaction { txn =>
  txn.query.filter(_.age BETWEEN 20 -> 29).toList.map { peopleIn20s =>
    println(s"People in their 20s: $peopleIn20s")
  }
}.sync()
// People in their 20s: List(Person(Adam,21,None,Set(),List(),StringId(IDmTU51mzoBQCEyaxBuHrwtLEcmHTags)), Person(Adam,21,None,Set(),List(),StringId(KGrBn5aofL4Nr9U3rhfv3dFHFiZQLBBp)), Person(Adam,21,None,Set(),List(),StringId(zKsjLb0Oh67NU7cXuCqefzuYqEkLNYou)), Person(Adam,21,None,Set(),List(),StringId(YtDDj7Lf0ys2sVAl5KbaGwYX1cRJdV41)), Person(Adam,21,None,Set(),List(),StringId(JzoJoBINhzejipsrAYzdaUGVvlxEFW5g)), Person(Adam,21,None,Set(),List(),StringId(5o9UsGhDtjTKVOLvHZCg0Y9CYjoh5g7C)), Person(Adam,21,None,Set(),List(),StringId(SpOTvdzPy3w302cWeQXRvtuVrJFDm13Z)), Person(Adam,21,None,Set(),List(),StringId(9WD5mBb0Y5IXtF2vuDa7fi8Y0pSw0Da0)), Person(Adam,21,None,Set(),List(),StringId(1gh7JtBVdDNqjihBDogvU4NNRGPJsXkb)), Person(Adam,21,None,Set(),List(),StringId(Xa6wUoSrdhjLP2vkbKyiyUjlyWBAz4kD)), Person(Adam,21,None,Set(),List(),StringId(iT74rK8QvkrRf6DrevkvgwQcHRgFuoUE)), Person(Adam,21,None,Set(),List(),StringId(QLvnBifleraDeNmCHkKeIPqyzhnib2Eg)), Person(Adam,21,None,Set(),List(),StringId(SJAjOvPNYLRg5wQ00zxEZUOUES7tCxcP)), Person(Adam,21,None,Set(),List(),StringId(zdX8DTpGZyn3MkGJhHaKnUz1cu9ZUdrK)), Person(Adam,21,None,Set(),List(),StringId(rdsWgq0lgl2jDHbivox9Vfz20zQ9Oe9L)), Person(Adam,21,None,Set(),List(),StringId(W7eeWhwhqCkihCVVVgujwUFDkhEO3oRa)), Person(Adam,21,None,Set(),List(),StringId(cMMqWQP2BIdaQp51oCPxVenB4ulAtVWl)), Person(Adam,21,None,Set(),List(),StringId(M0icIK9ngQkZcxcHFWKNQjzGcxnKq5SV)), Person(Adam,21,None,Set(),List(),StringId(AAGmPac35fkhX4pwacUuJ6lk0syGxFvk)))
```

---

## Features Highlight

1. **Transactions:**
   LightDB ensures atomic operations within transactions.

2. **Indexes:**
   Support for various indexes, like tokenized and field-based, ensures fast lookups.

3. **Aggregation:**
   Perform aggregations such as `min`, `max`, `avg`, and `sum`.

4. **Streaming:**
   Stream records for large-scale queries.

5. **Backups and Restores:**
   Backup and restore databases seamlessly.

6. **Prefix-Scanned File Storage (chunked blobs):**
   Store file metadata under `file:<id>` and data chunks under `data::<id>::<chunk>`. Requires a prefix-capable store: RocksDB, LMDB, or MapDB (B-Tree).

---

## Advanced Queries

### Aggregations

```scala
db.people.transaction { txn =>
  txn.query
    .aggregate(p => List(p.age.min, p.age.max, p.age.avg, p.age.sum))
    .toList
    .map { results =>
      println(s"Results: $results")
    }
}.sync()
// Results: List(MaterializedAggregate({"ageMin": 21, "ageMax": 21, "ageAvg": 21.0, "ageSum": 399},repl.MdocSession$MdocApp$Person$@237a6f48))
```

### Grouping

```scala
db.people.transaction { txn =>
  txn.query.grouped(_.age).toList.map { grouped =>
    println(s"Grouped: $grouped")
  }
}.sync()
// Grouped: List(Grouped(21,List(Person(Adam,21,None,Set(),List(),StringId(IDmTU51mzoBQCEyaxBuHrwtLEcmHTags)), Person(Adam,21,None,Set(),List(),StringId(KGrBn5aofL4Nr9U3rhfv3dFHFiZQLBBp)), Person(Adam,21,None,Set(),List(),StringId(zKsjLb0Oh67NU7cXuCqefzuYqEkLNYou)), Person(Adam,21,None,Set(),List(),StringId(YtDDj7Lf0ys2sVAl5KbaGwYX1cRJdV41)), Person(Adam,21,None,Set(),List(),StringId(JzoJoBINhzejipsrAYzdaUGVvlxEFW5g)), Person(Adam,21,None,Set(),List(),StringId(5o9UsGhDtjTKVOLvHZCg0Y9CYjoh5g7C)), Person(Adam,21,None,Set(),List(),StringId(SpOTvdzPy3w302cWeQXRvtuVrJFDm13Z)), Person(Adam,21,None,Set(),List(),StringId(9WD5mBb0Y5IXtF2vuDa7fi8Y0pSw0Da0)), Person(Adam,21,None,Set(),List(),StringId(1gh7JtBVdDNqjihBDogvU4NNRGPJsXkb)), Person(Adam,21,None,Set(),List(),StringId(Xa6wUoSrdhjLP2vkbKyiyUjlyWBAz4kD)), Person(Adam,21,None,Set(),List(),StringId(iT74rK8QvkrRf6DrevkvgwQcHRgFuoUE)), Person(Adam,21,None,Set(),List(),StringId(QLvnBifleraDeNmCHkKeIPqyzhnib2Eg)), Person(Adam,21,None,Set(),List(),StringId(SJAjOvPNYLRg5wQ00zxEZUOUES7tCxcP)), Person(Adam,21,None,Set(),List(),StringId(zdX8DTpGZyn3MkGJhHaKnUz1cu9ZUdrK)), Person(Adam,21,None,Set(),List(),StringId(rdsWgq0lgl2jDHbivox9Vfz20zQ9Oe9L)), Person(Adam,21,None,Set(),List(),StringId(W7eeWhwhqCkihCVVVgujwUFDkhEO3oRa)), Person(Adam,21,None,Set(),List(),StringId(cMMqWQP2BIdaQp51oCPxVenB4ulAtVWl)), Person(Adam,21,None,Set(),List(),StringId(M0icIK9ngQkZcxcHFWKNQjzGcxnKq5SV)), Person(Adam,21,None,Set(),List(),StringId(AAGmPac35fkhX4pwacUuJ6lk0syGxFvk)))))
```

---

## Backup and Restore

Backup your database:

```scala
import lightdb.backup._
import java.io.File

DatabaseBackup.archive(db.stores, new File("backup.zip")).sync()
// res6: Int = 20
```

Restore from a backup:

```scala
DatabaseRestore.archive(db, new File("backup.zip")).sync()
// res7: Int = 20
```

---

## File Storage (prefix, chunked)

Prefix-capable stores only: RocksDB, LMDB, MapDB (B-Tree). Metadata lives at `file:<id>`, chunks at `data::<id>::<chunk>`, enabling ordered streaming by chunk index.

```scala
import lightdb.file.FileStorage
import lightdb.rocksdb.RocksDBStore    // or LMDBStore / MapDBStore
import lightdb.KeyValue
import rapid.Stream
import java.nio.file.Path

object fileDb extends LightDB {
  override type SM = RocksDBStore.type
  override val storeManager: RocksDBStore.type = RocksDBStore
  override val directory = Some(Path.of("db/files"))
  override def upgrades = Nil
}

fileDb.init.sync()

// Use a dedicated KeyValue store for files (prefix-capable manager required)
val fs = FileStorage(fileDb, "_files")

// Write (chunk size = 4 bytes)
val meta = fs.put("hello.txt", Stream.emits(Seq("Hello RocksDB!".getBytes("UTF-8"))), chunkSize = 4).sync()

// Read back
val bytes = fs.readAll(meta.fileId).sync().flatten
println(new String(bytes, "UTF-8")) // Hello RocksDB!

// List and delete
fs.list.sync().map(_.fileName)
fs.delete(meta.fileId).sync()
```

---

## Full-Text Search (Lucene)

```scala
import lightdb._
import lightdb.lucene.LuceneStore
import lightdb.doc._
import lightdb.id.Id
import fabric.rw._
import java.nio.file.Path

case class Note(text: String, _id: Id[Note] = Id()) extends Document[Note]
object Note extends DocumentModel[Note] with JsonConversion[Note] {
  implicit val rw: RW[Note] = RW.gen
  val text = field.tokenized("text", _.text)
}

object luceneDb extends LightDB {
  type SM = LuceneStore.type
  val storeManager = LuceneStore
  val directory = Some(Path.of("db/lucene"))
  val notes = store(Note)
  def upgrades = Nil
}

luceneDb.init.sync()
luceneDb.notes.transaction(_.insert(Note("the quick brown fox"))).sync()
// res9: Note = Note(
//   text = "the quick brown fox",
//   _id = StringId("4OqCXpl3ZZCf7CI4YIeD3OtbxLEQqtYy")
// )
val hits = luceneDb.notes.transaction { txn =>
  txn.query.search.flatMap(_.list)
}.sync()
// hits: List[Note] = List(
//   Note(
//     text = "the quick brown fox",
//     _id = StringId("KFZdfuQF6mqDk4l1xVggnZduOBEqCkdD")
//   ),
//   Note(
//     text = "the quick brown fox",
//     _id = StringId("mioULpUq2rDeJOGqdeGxckoN8HpWL1rY")
//   ),
//   Note(
//     text = "the quick brown fox",
//     _id = StringId("jG4vbbyuCaioTXX3GpJV8pxfkcVMbxez")
//   ),
//   Note(
//     text = "the quick brown fox",
//     _id = StringId("cGw4Y83JUCTcaJHV7GWboFJ5CPGD9rpO")
//   ),
//   Note(
//     text = "the quick brown fox",
//     _id = StringId("2wCZpCOI4OfTMwqVAKFCijhGbv9MKnPM")
//   ),
//   Note(
//     text = "the quick brown fox",
//     _id = StringId("luPUAWZ2XRxcPvChsiGAt1MEJB1rEr8D")
//   ),
//   Note(
//     text = "the quick brown fox",
//     _id = StringId("ty8fBVxom4b4m12diBkul8aZ03AX7z83")
//   ),
//   Note(
//     text = "the quick brown fox",
//     _id = StringId("4OqCXpl3ZZCf7CI4YIeD3OtbxLEQqtYy")
//   )
// )
```

## Spatial Queries

```scala
import lightdb._
import lightdb.doc._
import lightdb.id.Id
import lightdb.spatial.Point
import lightdb.distance._
import lightdb.sql.SQLiteStore
import fabric.rw._
import java.nio.file.Path

case class Place(name: String, loc: Point, _id: Id[Place] = Id()) extends Document[Place]
object Place extends DocumentModel[Place] with JsonConversion[Place] {
  implicit val rw: RW[Place] = RW.gen
  val name = field("name", _.name)
  val loc  = field.index("loc", _.loc) // index for spatial queries
}

object spatialDb extends LightDB {
  type SM = SQLiteStore.type
  val storeManager = SQLiteStore
  val directory = Some(Path.of("db/spatial"))
  val places = store(Place)
  def upgrades = Nil
}

spatialDb.init.sync()
spatialDb.places.transaction(_.insert(Place("NYC", Point(40.7142, -74.0119)))).sync()
// res11: Place = Place(
//   name = "NYC",
//   loc = Point(latitude = 40.7142, longitude = -74.0119),
//   _id = StringId("Lajurby5inMeqzLnSjZtfsUtJFWVrmPe")
// )
// Distance filters are supported on spatial-capable backends; example filter:
val nycFilter = Place.loc.distance(Point(40.7, -74.0), 5_000.meters)
// nycFilter: Filter[Place] = Distance(
//   fieldName = "loc",
//   from = Point(latitude = 40.7, longitude = -74.0),
//   radius = Distance(5000.0)
// )
```

## Graph Traversal (Edges)

```scala
import lightdb._
import lightdb.doc._
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.id.Id
import fabric.rw._
import java.nio.file.Path

case class GPerson(name: String, _id: Id[GPerson] = Id()) extends Document[GPerson]
object GPerson extends DocumentModel[GPerson] with JsonConversion[GPerson] {
  implicit val rw: RW[GPerson] = RW.gen
  val name = field("name", _.name)
}

case class Follows(_from: Id[GPerson], _to: Id[GPerson]) extends EdgeDocument[Follows, GPerson, GPerson] {
  override val _id: EdgeId[Follows, GPerson, GPerson] = EdgeId(_from, _to)
}
object Follows extends EdgeModel[Follows, GPerson, GPerson] with JsonConversion[Follows] {
  implicit val rw: RW[Follows] = RW.gen
}

object graphDb extends LightDB {
  type SM = lightdb.store.hashmap.HashMapStore.type
  val storeManager = lightdb.store.hashmap.HashMapStore
  val directory = None
  val people = store(GPerson)
  val follows = store(Follows)
  def upgrades = Nil
}

graphDb.init.sync()
```

## Split Collection (storage + search)

```scala
import lightdb._
import lightdb.doc._
import lightdb.store.split.SplitStoreManager
import lightdb.rocksdb.RocksDBStore
import lightdb.lucene.LuceneStore
import fabric.rw._
import java.nio.file.Path

case class Article(title: String, body: String, _id: Id[Article] = Id()) extends Document[Article]
object Article extends DocumentModel[Article] with JsonConversion[Article] {
  implicit val rw: RW[Article] = RW.gen
  val title = field.index("title", _.title)
  val body  = field.tokenized("body", _.body)
}

object splitDb extends LightDB {
  type SM = SplitStoreManager[lightdb.rocksdb.RocksDBStore.type, lightdb.lucene.LuceneStore.type]
  val storeManager = SplitStoreManager(RocksDBStore, LuceneStore)
  val directory = Some(Path.of("db/split"))
  val articles = store(Article)
  def upgrades = Nil
}
```

## Sharded / MultiStore

```scala
import lightdb._
import lightdb.doc._
import lightdb.store.hashmap.HashMapStore
import fabric.rw._

case class TenantDoc(value: String, _id: Id[TenantDoc] = Id()) extends Document[TenantDoc]
object TenantDoc extends DocumentModel[TenantDoc] with JsonConversion[TenantDoc] {
  implicit val rw: RW[TenantDoc] = RW.gen
  val value = field("value", _.value)
}

object shardDb extends LightDB {
  type SM = HashMapStore.type
  val storeManager = HashMapStore
  val directory = None
  def upgrades = Nil
  val shards = multiStore[String, TenantDoc, TenantDoc.type](TenantDoc, key => s"tenant_$key")
}

shardDb.init.sync()
val tenantA = shardDb.shards("tenantA")
// tenantA: HashMapStore[TenantDoc, TenantDoc] = lightdb.store.hashmap.HashMapStore@307c3915
tenantA.transaction(_.insert(TenantDoc("hello"))).sync()
// res14: TenantDoc = TenantDoc(
//   value = "hello",
//   _id = StringId("7EzO9h1h7ev9RBvL7pA5Ka1sjwNvbN5s")
// )
```

## Stored Values (config flags)

```scala
import lightdb._
import fabric.rw._

object cfgDb extends LightDB {
  type SM = lightdb.store.hashmap.HashMapStore.type
  val storeManager = lightdb.store.hashmap.HashMapStore
  val directory = None
  def upgrades = Nil
}

cfgDb.init.sync()
val featureFlag = cfgDb.stored[Boolean]("featureX", default = false)
// featureFlag: StoredValue[Boolean] = StoredValue(
//   key = "featureX",
//   store = lightdb.store.hashmap.HashMapStore@6fd1ade2,
//   default = repl.MdocSession$MdocApp$$Lambda/0x00000000147e9668@4fff1c49,
//   persistence = Stored
// )
featureFlag.set(true).sync()
// res16: Boolean = true
```

## SQL Stores (DuckDB / SQLite)

```scala
import lightdb._
import lightdb.doc._
import lightdb.id.Id
import lightdb.sql.SQLiteStore
import fabric.rw._
import java.nio.file.Path

case class Row(value: String, _id: Id[Row] = Id()) extends Document[Row]
object Row extends DocumentModel[Row] with JsonConversion[Row] {
  implicit val rw: RW[Row] = RW.gen
  val value = field("value", _.value)
}

object sqlDb extends LightDB {
  type SM = SQLiteStore.type
  val storeManager = SQLiteStore
  val directory = Some(Path.of("db/sqlite-example"))
  val rows = store(Row)
  def upgrades = Nil
}

sqlDb.init.sync()
sqlDb.rows.transaction(_.insert(Row("hi sql"))).sync()
// res18: Row = Row(
//   value = "hi sql",
//   _id = StringId("6ms7NkWmMH6mazvlxWVRo75d0vvdoBSO")
// )
```

## Reindex / Optimize / Upgrades

- `store.reIndex()` and `store.optimize()` give backends a chance to rebuild or compact data.
- Database upgrades: implement `upgrades: List[DatabaseUpgrade]` and add migration steps; LightDB runs them on init.

## Clean Up

Dispose of the database when done:

```scala
db.dispose.sync()
```

---

## Conclusion

This guide provided an overview of using **LightDB**. Experiment with its features to explore the full potential of this high-performance database. For advanced use cases, consult the API documentation.