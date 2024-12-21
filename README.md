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

## SBT Configuration

To add all modules:
```scala
libraryDependencies += "com.outr" %% "lightdb-all" % "1.2.2"
```

For a specific implementation like Lucene:
```scala
libraryDependencies += "com.outr" %% "lightdb-lucene" % "1.2.2"
```

## Videos
Watch this [Java User Group demonstration of LightDB](https://www.youtube.com/live/E_5fwgbF4rc?si=cxyb0Br3oCEQInTW)

## Getting Started

This guide will walk you through setting up and using **LightDB**, a high-performance computational database. We'll use a sample application to explore its key features.

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
libraryDependencies += "com.outr" %% "lightdb-all" % "1.2.2"
```

---

## Example: Defining Models and Collections

### Step 1: Define Your Models

LightDB uses **Document** and **DocumentModel** for schema definitions. Here's an example of defining a `Person` and `City`:

```scala
import lightdb._
import lightdb.collection._
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

Define the database with collections for each model:

```scala
import lightdb.sql._
import lightdb.store._
import lightdb.upgrade._
import java.nio.file.Path

class DB extends LightDB {
  lazy val directory: Option[Path] = Some(Path.of(s"db/example"))
   
  lazy val people: Collection[Person, Person.type] = collection(Person)

  override def storeManager: StoreManager = SQLiteStore

  override def upgrades: List[DatabaseUpgrade] = Nil
}
```

---

## Using the Database

### Step 1: Initialize the Database

Instantiate and initialize the database:

```scala
val db = new DB
// db: DB = repl.MdocSession$MdocApp$DB@6e580455
db.init()
// res0: Boolean = true
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
//   _id = Id(value = "RgelUWnuRJLktKoZMOwivwanvRhRWSoT")
// )
db.people.transaction { implicit transaction =>
  db.people.insert(adam)
}
// res1: Person = Person(
//   name = "Adam",
//   age = 21,
//   city = None,
//   nicknames = Set(),
//   friends = List(),
//   _id = Id(value = "RgelUWnuRJLktKoZMOwivwanvRhRWSoT")
// )
```

### Step 3: Query Data

Retrieve records using filters:

```scala
db.people.transaction { implicit transaction =>
  val peopleIn20s = db.people.query.filter(_.age BETWEEN 20 -> 29).toList
  println(peopleIn20s)
}
// List(Person(Adam,21,None,Set(),List(),Id(V4HuAlFgFWP0bARChPFtCx5eqMCvtX7l)), Person(Adam,21,None,Set(),List(),Id(xxVmia6PxlkFL1nWWDNFKiO2KccbDrhv)), Person(Adam,21,None,Set(),List(),Id(eaQIHd0ZiDHjxWa9VXzhybMHWtH80C47)), Person(Adam,21,None,Set(),List(),Id(Mg3nibsB1wstqK1xEIiGNeU4Q5iw7Kfc)), Person(Adam,21,None,Set(),List(),Id(bKaLE07r8AzMoAjBotiP0dDfo9n96I1q)), Person(Adam,21,None,Set(),List(),Id(Kq2SaCpZ7spHyMnYT2IvmpbynonZEBR2)), Person(Adam,21,None,Set(),List(),Id(lWJ5yg8AquThmEQ5yoRVOTrVzpfAP4II)), Person(Adam,21,None,Set(),List(),Id(mF9hZfb8wM4meAcQeUKfAdZ75dELpE81)), Person(Adam,21,None,Set(),List(),Id(KX7pnHxh7qet74BmcULK6XK9lkcxnTCc)), Person(Adam,21,None,Set(),List(),Id(4dsT7zjp8OTm2i6RoKVCrYlsjXKvtXHe)), Person(Adam,21,None,Set(),List(),Id(BzUQdoR76YpFIwxdGpG3kR6WMLvNZxPr)), Person(Adam,21,None,Set(),List(),Id(RgelUWnuRJLktKoZMOwivwanvRhRWSoT)))
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
  val results = db.people.query
    .aggregate(p => List(p.age.min, p.age.max, p.age.avg, p.age.sum))
    .toList
  println(results)
}
// List(MaterializedAggregate({"ageMin": 21, "ageMax": 21, "ageAvg": 21.0, "ageSum": 252},repl.MdocSession$MdocApp$Person$@8caf39))
```

### Grouping

```scala
db.people.transaction { implicit transaction =>
  val grouped = db.people.query.grouped(_.age).toList
  println(grouped)
}
// List((21,List(Person(Adam,21,None,Set(),List(),Id(V4HuAlFgFWP0bARChPFtCx5eqMCvtX7l)), Person(Adam,21,None,Set(),List(),Id(xxVmia6PxlkFL1nWWDNFKiO2KccbDrhv)), Person(Adam,21,None,Set(),List(),Id(eaQIHd0ZiDHjxWa9VXzhybMHWtH80C47)), Person(Adam,21,None,Set(),List(),Id(Mg3nibsB1wstqK1xEIiGNeU4Q5iw7Kfc)), Person(Adam,21,None,Set(),List(),Id(bKaLE07r8AzMoAjBotiP0dDfo9n96I1q)), Person(Adam,21,None,Set(),List(),Id(Kq2SaCpZ7spHyMnYT2IvmpbynonZEBR2)), Person(Adam,21,None,Set(),List(),Id(lWJ5yg8AquThmEQ5yoRVOTrVzpfAP4II)), Person(Adam,21,None,Set(),List(),Id(mF9hZfb8wM4meAcQeUKfAdZ75dELpE81)), Person(Adam,21,None,Set(),List(),Id(KX7pnHxh7qet74BmcULK6XK9lkcxnTCc)), Person(Adam,21,None,Set(),List(),Id(4dsT7zjp8OTm2i6RoKVCrYlsjXKvtXHe)), Person(Adam,21,None,Set(),List(),Id(BzUQdoR76YpFIwxdGpG3kR6WMLvNZxPr)), Person(Adam,21,None,Set(),List(),Id(RgelUWnuRJLktKoZMOwivwanvRhRWSoT)))))
```

---

## Backup and Restore

Backup your database:

```scala
import lightdb.backup._
import java.io.File

DatabaseBackup.archive(db, new File("backup.zip"))
// res5: Int = 13
```

Restore from a backup:

```scala
DatabaseRestore.archive(db, new File("backup.zip"))
// res6: Int = 13
```

---

## Clean Up

Dispose of the database when done:

```scala
db.dispose()
```

---

## Conclusion

This guide provided an overview of using **LightDB**. Experiment with its features to explore the full potential of this high-performance database. For advanced use cases, consult the API documentation.