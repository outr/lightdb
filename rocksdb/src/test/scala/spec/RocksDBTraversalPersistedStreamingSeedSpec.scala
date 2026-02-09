package spec

import fabric.rw.*
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort}
import lightdb.traversal.store.TraversalManager
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.AsyncTaskSpec
import lightdb.filter.FilterExtras
import scala.language.implicitConversions

import java.nio.file.Path

@EmbeddedTest
class RocksDBTraversalPersistedStreamingSeedSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with TraversalRocksDBWrappedManager
    with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    System.setProperty("lightdb.traversal.persistedIndex.maxSeedSize", "1")
    System.setProperty("lightdb.traversal.persistedIndex.streamingSeed.enabled", "true")
    // Exercise bounded refinement for Multi streaming seed (intersect small MUST/FILTER postings sets before verify).
    System.setProperty("lightdb.traversal.persistedIndex.streamingSeed.multi.refine.enabled", "true")
    System.setProperty("lightdb.traversal.persistedIndex.streamingSeed.multi.refineExclude.enabled", "true")
    System.setProperty("lightdb.traversal.persistedIndex.streamingSeed.multi.refine.maxIds", "1000")
    System.setProperty("lightdb.traversal.persistedIndex.streamingSeed.multi.refine.maxClauses", "2")
    // Keep oversampling minimal to exercise correctness fallback when postings are approximate.
    System.setProperty("lightdb.traversal.persistedIndex.streamingSeed.indexOrder.rangeOversample", "1")
    System.setProperty("lightdb.traversal.persistedIndex.streamingSeed.indexOrder.prefixOversample", "1")
    // Small chunk size to exercise chunked In filter path with few values.
    System.setProperty("lightdb.traversal.persistedIndex.streamingSeed.maxInTerms", "2")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally {
      System.clearProperty("lightdb.traversal.persistedIndex.maxSeedSize")
      System.clearProperty("lightdb.traversal.persistedIndex.streamingSeed.enabled")
      System.clearProperty("lightdb.traversal.persistedIndex.streamingSeed.indexOrder.rangeOversample")
      System.clearProperty("lightdb.traversal.persistedIndex.streamingSeed.indexOrder.prefixOversample")
      System.clearProperty("lightdb.traversal.persistedIndex.streamingSeed.maxInTerms")
    }
  }

  private lazy val specName: String = getClass.getSimpleName
  override def traversalStoreManager: TraversalManager = super.traversalStoreManager

  object DB extends LightDB {
    override type SM = TraversalManager
    override val storeManager: TraversalManager = traversalStoreManager

    override def name: String = specName
    override lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val people: S[Person, Person.type] = store(Person)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Person(name: String,
                    age: Int,
                    rank: Long = 0L,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Person] = Person.id()) extends RecordDocument[Person]

  object Person extends RecordDocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen
    val age: I[Int] = field.index(_.age)
    val rank: I[Long] = field.index(_.rank)
    val name: I[String] = field.index(_.name)
  }

  specName should {
    "use persisted streaming seed for page-only equals when seed materialization is skipped" in {
      val docs = (1 to 50).toList.map(i => Person(name = s"p$i", age = if i <= 40 then 1 else 2, _id = Id(s"p$i")))
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .limit(1)
            .sort(Sort.IndexOrder)
            .filter(_.age === 1)
            .search
        }
        list <- results.stream.toList
      yield {
        list.size shouldBe 1
        list.head.age shouldBe 1
      }
    }

    "use persisted streaming seed for page-only startsWith when seed materialization is skipped" in {
      val docs = (1 to 50).toList.map(i => Person(name = if i <= 40 then s"abc$i" else s"zzz$i", age = i, _id = Id(s"s$i")))
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .sort(Sort.IndexOrder)
            .limit(1)
            .filter(_.name.startsWith("abc"))
            .search
        }
        list <- results.stream.toList
      yield {
        list.size shouldBe 1
        list.head.name.startsWith("abc") shouldBe true
      }
    }

    "fall back when IndexOrder startsWith postings are too approximate to fill the page under oversample=1" in {
      // prefixMaxLen defaults to 8; query is longer, so swo postings are a superset and require verification.
      val q = "abcdefghij"
      val good = (1 to 10).toList.map(i => Person(name = s"${q}__ok$i", age = i, _id = Id(f"z$i%03d")))
      val bad = (1 to 200).toList.map(i => Person(name = s"abcdefghXX_bad$i", age = i, _id = Id(f"a$i%03d")))
      val docs = bad ++ good
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .sort(Sort.IndexOrder)
            .limit(5)
            .filter(_.name.startsWith(q))
            .search
        }
        list <- results.stream.toList
      yield {
        list.size shouldBe 5
        all(list.map(_.name.startsWith(q))) shouldBe true
      }
    }

    "support Multi filter by streaming a driver clause and verifying the full filter (IndexOrder + oversample=1)" in {
      val q = "abcdefghij"
      val good = (1 to 10).toList.map(i => Person(name = s"${q}__ok$i", age = 1, _id = Id(f"z2$i%03d")))
      val bad = (1 to 200).toList.map(i => Person(name = s"abcdefghXX_bad$i", age = 1, _id = Id(f"a2$i%03d")))
      val docs = bad ++ good
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .sort(Sort.IndexOrder)
            .limit(5)
            .filter(p => (p.age === 1) && p.name.startsWith(q))
            .search
        }
        list <- results.stream.toList
      yield {
        list.size shouldBe 5
        all(list.map(_.age == 1)) shouldBe true
        all(list.map(_.name.startsWith(q))) shouldBe true
      }
    }

    "support Multi filter with MUST_NOT refinement (IndexOrder)" in {
      val q = "keepPrefix"
      val good = (1 to 10).toList.map(i => Person(name = s"${q}__ok$i", age = 1, _id = Id(f"z9$i%03d")))
      val excluded = (1 to 200).toList.map(i => Person(name = s"${q}__bad$i", age = 1, rank = 999L, _id = Id(f"a9$i%03d")))
      // Add extra docs that startWith(q) but fail age==1 to force driver selection toward Equals(age==1) (smaller postings count).
      val otherStarting = (1 to 50).toList.map(i => Person(name = s"${q}__other$i", age = 2, rank = 0L, _id = Id(f"b9$i%03d")))
      val other = (1 to 50).toList.map(i => Person(name = s"other$i", age = 2, rank = 0L, _id = Id(f"c9$i%03d")))
      val docs = excluded ++ good ++ otherStarting ++ other
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          import lightdb.filter.{Condition, Filter, FilterClause}
          tx.query
            .clearPageSize
            .sort(Sort.IndexOrder)
            .limit(5)
            .filter { p =>
              Filter.Multi(
                minShould = 0,
                filters = List(
                  FilterClause(p.age === 1, Condition.Must, None),
                  FilterClause(p.name.startsWith(q), Condition.Must, None),
                  FilterClause(p.rank === 999L, Condition.MustNot, None)
                )
              )
            }
            .search
        }
        list <- results.stream.toList
      yield {
        list.size shouldBe 5
        all(list.map(_.age == 1)) shouldBe true
        all(list.map(_.name.startsWith(q))) shouldBe true
        all(list.map(_.rank != 999L)) shouldBe true
      }
    }

    "support OR-style Multi (pure SHOULD) by merging seedable SHOULD streams (IndexOrder + maxSeedSize=1)" in {
      val q1 = "prefixAAAAAA"
      val q2 = "prefixBBBBBB"
      val good1 = (1 to 10).toList.map(i => Person(name = s"${q1}__ok$i", age = 1, _id = Id(f"z3$i%03d")))
      val good2 = (1 to 10).toList.map(i => Person(name = s"${q2}__ok$i", age = 1, _id = Id(f"z4$i%03d")))
      val bad = (1 to 200).toList.map(i => Person(name = s"prefixXXXXXX_bad$i", age = 1, _id = Id(f"a3$i%03d")))
      val docs = bad ++ good1 ++ good2
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .sort(Sort.IndexOrder)
            .limit(5)
            .filter(p => p.name.startsWith(q1) || p.name.startsWith(q2))
            .search
        }
        list <- results.stream.toList
      yield {
        list.size shouldBe 5
        all(list.map(p => p.name.startsWith(q1) || p.name.startsWith(q2))) shouldBe true
      }
    }

    "support OR-style Multi (pure SHOULD) with MUST_NOT exclusion (IndexOrder)" in {
      val q1 = "orX"
      val q2 = "orY"
      val docs =
        (1 to 50).toList.map(i => Person(name = s"${q1}_$i", age = 1, rank = if i <= 10 then 999L else 1L, _id = Id(f"o1$i%03d"))) ++
          (1 to 50).toList.map(i => Person(name = s"${q2}_$i", age = 1, rank = 1L, _id = Id(f"o2$i%03d")))

      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          import lightdb.filter.{Condition, Filter, FilterClause}
          tx.query
            .clearPageSize
            .sort(Sort.IndexOrder)
            .limit(10)
            .filter { p =>
              Filter.Multi(
                minShould = 1,
                filters = List(
                  FilterClause(p.name.startsWith(q1), Condition.Should, None),
                  FilterClause(p.name.startsWith(q2), Condition.Should, None),
                  FilterClause(p.rank === 999L, Condition.MustNot, None)
                )
              )
            }
            .search
        }
        list <- results.stream.toList
      yield {
        list.size shouldBe 10
        all(list.map(p => p.name.startsWith(q1) || p.name.startsWith(q2))) shouldBe true
        all(list.map(_.rank != 999L)) shouldBe true
      }
    }

    "support Multi IndexOrder intersection for RangeLong + Equals (page-only) with maxSeedSize=1" in {
      val docs = (1 to 200).toList.map { i =>
        Person(name = s"p$i", age = if i % 2 == 0 then 1 else 2, rank = i.toLong, _id = Id(f"m$i%04d"))
      }
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .limit(5)
            .sort(Sort.IndexOrder)
            .filter(m => (m.rank >= 50L) && (m.rank <= 150L))
            .filter(_.age === 1)
            .search
        }
        list <- results.stream.toList
      yield {
        list.nonEmpty shouldBe true
        all(list.map(_.age)) shouldBe 1
        all(list.map(_.rank)) should (be >= 50L and be <= 150L)
      }
    }

    "preserve IndexOrder for OR-style Multi even when smallest SHOULD clause has higher docIds" in {
      // Clause ordering will prefer the smaller postings set (the 'z' clause), but IndexOrder must still return 'a' ids first.
      val aDocs = (1 to 50).toList.map(i => Person(name = s"a$i", age = 1, _id = Id(f"a$i%03d")))
      val zDoc = Person(name = "z", age = 1, _id = Id("z999"))
      val docs = aDocs :+ zDoc
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .sort(Sort.IndexOrder)
            .limit(5)
            .filter(p => p.name.startsWith("z") || p.name.startsWith("a"))
            .search
        }
        list <- results.stream.toList
      yield {
        list.map(_._id.value) shouldBe List("a001", "a002", "a003", "a004", "a005")
      }
    }

    "use persisted streaming seed for page-only IN when seed materialization is skipped" in {
      val docs = (1 to 60).toList.map(i => Person(name = s"n$i", age = if i <= 50 then 1 else 2, _id = Id(s"i$i")))
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .sort(Sort.IndexOrder)
            .limit(3)
            .filter(_.age.in(Seq(1, 2)))
            .search
        }
        list <- results.stream.toList
      yield {
        list.size shouldBe 3
        all(list.map(_.age)) should (be(1) or be(2))
      }
    }

    "use chunked streaming seed for In filter with more values than streamingMaxInTerms (no sort)" in {
      // maxInTerms is set to 2 in beforeAll(), so 4 In values create 2 chunks.
      val docs = (1 to 6).toList.map(i => Person(name = s"chk$i", age = i, _id = Id(s"chk$i")))
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        // 4 distinct In values -> 2 chunks of 2 with maxInTerms=2
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .limit(3)
            .filter(_.age.in(Seq(1, 2, 3, 4)))
            .search
        }
        list <- results.stream.toList
      yield {
        list.size shouldBe 3
        all(list.map(a => a.age >= 1 && a.age <= 4)) shouldBe true
      }
    }

    "use persisted streaming seed for page-only rangeLong when seed materialization is skipped" in {
      // Intentionally choose docIds so lexicographic order starts with many out-of-range docs
      // (to validate oversampling for approximate IndexOrder range postings).
      val docs =
        (1 to 200).toList.map { i =>
          val id =
            if i < 50 then Id[Person](f"a$i%03d") // out-of-range and lexicographically early
            else Id[Person](f"z$i%03d")        // in-range and lexicographically late
          Person(name = s"r$i", age = i, rank = i.toLong, _id = id)
        }
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .sort(Sort.IndexOrder)
            .limit(5)
            .filter(_.rank.BETWEEN(50L -> 150L))
            .search
        }
        list <- results.stream.toList
      yield {
        list.size shouldBe 5
        all(list.map(_.rank)) should (be >= 50L and be <= 150L)
      }
    }

    "use persisted streaming seed for page-only Contains when seed materialization is skipped" in {
      val docs = (1 to 50).toList.map(i =>
        Person(name = if i <= 30 then s"alice_$i" else s"zzz_$i", age = i, _id = Id(s"c$i"))
      )
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .limit(5)
            .filter(_.name.contains("alice"))
            .search
        }
        list <- results.stream.toList
      yield {
        list.size shouldBe 5
        all(list.map(_.name.contains("alice"))) shouldBe true
      }
    }

    "use persisted streaming seed for page-only Regex when seed materialization is skipped" in {
      val docs = (1 to 50).toList.map(i =>
        Person(name = if i <= 30 then s"alice_$i" else s"zzz_$i", age = i, _id = Id(s"rx$i"))
      )
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .limit(5)
            .filter(_.name.regex(".*alice.*"))
            .search
        }
        list <- results.stream.toList
      yield {
        list.size shouldBe 5
        all(list.map(_.name.contains("alice"))) shouldBe true
      }
    }

    "use seed fallback for filter not covered by streaming-seed match (custom sort)" in {
      // A Contains filter with a custom (non-empty, non-IndexOrder) sort does not match any
      // streaming-seed case. The seed fallback should use seedCandidatesPersisted instead of full scan.
      val docs = (1 to 50).toList.map(i =>
        Person(name = if i <= 30 then s"alice_$i" else s"zzz_$i", age = i, _id = Id(s"fb$i"))
      )
      for
        _ <- DB.init
        _ <- DB.people.transaction(_.truncate)
        _ <- DB.people.transaction(_.insert(docs))
        _ <- DB.people
          .asInstanceOf[lightdb.traversal.store.TraversalStore[_, _]]
          .buildPersistedIndex()
        results <- DB.people.transaction { tx =>
          tx.query
            .clearPageSize
            .limit(5)
            .sort(Sort.ByField(Person.age).asc)
            .filter(_.name.contains("alice"))
            .search
        }
        list <- results.stream.toList
      yield {
        list.size shouldBe 5
        all(list.map(_.name.contains("alice"))) shouldBe true
      }
    }
  }
}



