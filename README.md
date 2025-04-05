# lightdb
[![CI](https://github.com/outr/lightdb/actions/workflows/ci.yml/badge.svg)](https://github.com/outr/lightdb/actions/workflows/ci.yml)

Computationally focused database using pluggable stores

## Provided Stores
- Yahoo's HaloDB (https://github.com/yahoo/HaloDB)
- MapDB (https://mapdb.org)
- Facebook's RocksDB (https://rocksdb.org)
- Redis (https://redis.io)
- Apache Lucene (https://lucene.apache.org)
- SQLite (https://www.sqlite.org)
- H2 (https://h2database.com)
- DuckDB (https://duckdb.org)
- PostgreSQL (https://www.postgresql.org)
- Redis (https://redis.io)

## In-Progress
- Tantivy (https://github.com/quickwit-oss/tantivy) - Working on creating a wrapper around Rust's extremely fast alternative to Apache Lucene (See https://github.com/outr/scantivy)

## SBT Configuration

To add all modules:
```scala
libraryDependencies += "com.outr" %% "lightdb-all" % "3.0.0"
```

For a specific implementation like Lucene:
```scala
libraryDependencies += "com.outr" %% "lightdb-lucene" % "3.0.0"
```

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
libraryDependencies += "com.outr" %% "lightdb-all" % "3.0.0"
```

---

## Example: Defining Models and Collections

### Step 1: Define Your Models

LightDB uses **Document** and **DocumentModel** for schema definitions. Here's an example of defining a `Person` and `City`:

```scala
import lightdb._
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
//   _id = Id(value = "N8eiyZnq9xOMjVW49sZlBgPbVI0mvJlj")
// )
db.people.transaction { implicit transaction =>
  db.people.insert(adam)
}.sync()
// res1: Person = Person(
//   name = "Adam",
//   age = 21,
//   city = None,
//   nicknames = Set(),
//   friends = List(),
//   _id = Id(value = "N8eiyZnq9xOMjVW49sZlBgPbVI0mvJlj")
// )
```

### Step 3: Query Data

Retrieve records using filters:

```scala
db.people.transaction { implicit transaction =>
  db.people.query.filter(_.age BETWEEN 20 -> 29).toList.map { peopleIn20s =>
    println(s"People in their 20s: $peopleIn20s")
  }
}.sync()
// People in their 20s: List(Person(Adam,21,None,Set(),List(),Id(N8eiyZnq9xOMjVW49sZlBgPbVI0mvJlj)))
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

---

## Advanced Queries

### Aggregations

```scala
db.people.transaction { implicit transaction =>
  db.people.query
    .aggregate(p => List(p.age.min, p.age.max, p.age.avg, p.age.sum))
    .toList
    .map { results =>
      println(s"Results: $results")
    }
}.sync()
// Results: List(MaterializedAggregate({"ageMin": 21, "ageMax": 21, "ageAvg": 21.0, "ageSum": 21},repl.MdocSession$MdocApp$Person$@6a2d4fb1))
```

### Grouping

```scala
db.people.transaction { implicit transaction =>
  db.people.query.grouped(_.age).toList.map { grouped =>
    println(s"Grouped: $grouped")
  }
}.sync()
// Grouped: List(Grouped(21,List(Person(Adam,21,None,Set(),List(),Id(N8eiyZnq9xOMjVW49sZlBgPbVI0mvJlj)))))
```

---

## Backup and Restore

Backup your database:

```scala
import lightdb.backup._
import java.io.File

DatabaseBackup.archive(db, new File("backup.zip")).sync()
// res5: Int = 2
```

Restore from a backup:

```scala
DatabaseRestore.archive(db, new File("backup.zip")).sync()
// res6: Int = 2
```

---

## Clean Up

Dispose of the database when done:

```scala
db.dispose.sync()
```

---

## Conclusion

This guide provided an overview of using **LightDB**. Experiment with its features to explore the full potential of this high-performance database. For advanced use cases, consult the API documentation.