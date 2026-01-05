package spec

import fabric._
import lightdb.LightDB
import lightdb.opensearch.{OpenSearchIndexMigration, OpenSearchJoinDomainCoordinator}
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import fabric.rw._
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

@EmbeddedTest
class OpenSearchJoinDomainMigrationSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  object DB extends LightDB {
    override type SM = lightdb.store.CollectionManager
    override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore

    override def name: String = "OpenSearchJoinDomainMigrationSpec"
    override lazy val directory: Option[Path] = Some(Path.of("db/OpenSearchJoinDomainMigrationSpec"))
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  "OpenSearch join-domain migrations" should {
    "reindex from join-domain read alias into a new physical index and swap join-domain aliases" in {
      val k1 = "lightdb.opensearch.useIndexAlias"
      val k2 = "lightdb.opensearch.useWriteAlias"
      val prev = Map(k1 -> Profig(k1).opt[String], k2 -> Profig(k2).opt[String])

      Profig(k1).store("true")
      Profig(k2).store("true")

      val joinDomain = "join_domain_migration_test"

      val config = OpenSearchConfig.from(DB, collectionName = "ParentStoreIgnored")
      val client = OpenSearchClient(config)

      val (readAlias, writeAlias) = OpenSearchJoinDomainCoordinator.joinDomainAliases(
        dbName = DB.name,
        joinDomain = joinDomain,
        config = config
      )

      val oldPhysical = s"${readAlias}_000001"
      val indexBody = obj("mappings" -> obj("dynamic" -> bool(true)))

      def matchAllCount(idx: String): Task[Int] =
        client.count(idx, obj("query" -> obj("match_all" -> obj())))

      val test = for {
        // cleanup from any prior run
        existingR <- client.aliasTargets(readAlias)
        _ <- existingR.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))
        existingW <- writeAlias match {
          case Some(wa) => client.aliasTargets(wa)
          case None => Task.pure(Nil)
        }
        _ <- existingW.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))
        _ <- client.deleteIndex(oldPhysical)

        // create initial index + aliases and index a couple docs
        _ <- client.createIndex(oldPhysical, indexBody)
        _ <- OpenSearchIndexMigration.repointReadWriteAliases(client, readAlias, writeAlias, oldPhysical)
        _ <- client.indexDoc(writeAlias.getOrElse(readAlias), "d1", obj("v" -> str("one")), refresh = Some("true"))
        _ <- client.indexDoc(writeAlias.getOrElse(readAlias), "d2", obj("v" -> str("two")), refresh = Some("true"))
        before <- matchAllCount(readAlias)

        // migrate to new physical index via join-domain wrapper (reindex + alias swap)
        newPhysical <- OpenSearchJoinDomainCoordinator.reindexAndRepointJoinDomainAliases(
          client = client,
          dbName = DB.name,
          joinDomain = joinDomain,
          config = config,
          newIndexBody = indexBody,
          defaultSuffix = "_000001"
        )
        after <- matchAllCount(readAlias)

        // cleanup
        _ <- client.deleteIndex(oldPhysical)
        _ <- client.deleteIndex(newPhysical)
      } yield {
        try {
          before should be(2)
          after should be(2)
          newPhysical should not be oldPhysical
        } finally {
          prev.foreach {
            case (k, Some(v)) => Profig(k).store(v)
            case (k, None) => Profig(k).remove()
          }
        }
      }

      test
    }
  }
}


