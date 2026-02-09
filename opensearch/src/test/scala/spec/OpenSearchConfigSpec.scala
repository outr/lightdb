package spec

import lightdb.LightDB
import lightdb.opensearch.OpenSearchStore
import lightdb.opensearch.OpenSearchJoinDomainRegistry
import lightdb.opensearch.client.OpenSearchConfig
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import profig.Profig
import fabric.rw.*

import java.nio.file.Path

@EmbeddedTest
class OpenSearchConfigSpec extends AnyWordSpec with Matchers with ProfigTestSupport {
  "OpenSearchConfig.from" should {
    "respect store-specific requestTimeoutMillis override" in {
      val storeName = "Doc"
      val key = s"lightdb.opensearch.$storeName.requestTimeoutMillis"
      val prev = Profig(key).opt[String]
      val prevBase = Profig("lightdb.opensearch.baseUrl").opt[String]

      Profig("lightdb.opensearch.baseUrl").store("http://localhost:9200")
      Profig(key).store("1234")

      class DB extends LightDB {
        override type SM = OpenSearchStore.type
        override val storeManager: OpenSearchStore.type = OpenSearchStore
        override def name: String = "OpenSearchConfigSpec"
        override lazy val directory: Option[Path] = None
        override def upgrades: List[DatabaseUpgrade] = Nil
      }

      val cfg = OpenSearchConfig.from(new DB, storeName)
      cfg.requestTimeout.toMillis shouldBe 1234L

      // restore
      prev match {
        case Some(v) => Profig(key).store(v)
        case None => Profig(key).remove()
      }
      prevBase match {
        case Some(v) => Profig("lightdb.opensearch.baseUrl").store(v)
        case None => Profig("lightdb.opensearch.baseUrl").remove()
      }
    }

    "infer child join config from a parent joinChildParentFields map (Profig-driven)" in {
      val prev = List(
        "lightdb.opensearch.baseUrl",
        "lightdb.opensearch.Parent.joinDomain",
        "lightdb.opensearch.Parent.joinChildren",
        "lightdb.opensearch.Parent.joinChildParentFields",
        "lightdb.opensearch.Child.joinDomain",
        "lightdb.opensearch.Child.joinRole",
        "lightdb.opensearch.Child.joinParentField"
      ).map(k => k -> Profig(k).opt[String]).toMap

      Profig("lightdb.opensearch.baseUrl").store("http://localhost:9200")
      Profig("lightdb.opensearch.Parent.joinDomain").store("join_cfg_spec")
      Profig("lightdb.opensearch.Parent.joinChildren").store("Child")
      Profig("lightdb.opensearch.Parent.joinChildParentFields").store("Child:parentId")
      Profig("lightdb.opensearch.Child.joinDomain").remove()
      Profig("lightdb.opensearch.Child.joinRole").remove()
      Profig("lightdb.opensearch.Child.joinParentField").remove()

      class DB extends LightDB {
        override type SM = OpenSearchStore.type
        override val storeManager: OpenSearchStore.type = OpenSearchStore
        override def name: String = "OpenSearchConfigSpec"
        override lazy val directory: Option[Path] = None
        override def upgrades: List[DatabaseUpgrade] = Nil
      }

      val childCfg = OpenSearchConfig.from(new DB, "Child")
      childCfg.joinDomain shouldBe Some("join_cfg_spec")
      childCfg.joinRole shouldBe Some("child")
      childCfg.joinParentField shouldBe Some("parentId")

      // restore
      prev.foreach {
        case (k, Some(v)) => Profig(k).store(v)
        case (k, None) => Profig(k).remove()
      }
    }

    "infer joinRole=parent when joinDomain is set and joinChildParentFields is present" in {
      val prev = List(
        "lightdb.opensearch.baseUrl",
        "lightdb.opensearch.Parent.joinDomain",
        "lightdb.opensearch.Parent.joinRole",
        "lightdb.opensearch.Parent.joinChildParentFields",
        "lightdb.opensearch.Parent.joinChildren"
      ).map(k => k -> Profig(k).opt[String]).toMap

      Profig("lightdb.opensearch.baseUrl").store("http://localhost:9200")
      Profig("lightdb.opensearch.Parent.joinDomain").store("join_cfg_spec")
      Profig("lightdb.opensearch.Parent.joinRole").remove()
      Profig("lightdb.opensearch.Parent.joinChildren").remove()
      Profig("lightdb.opensearch.Parent.joinChildParentFields").store("Child:parentId")

      class DB extends LightDB {
        override type SM = OpenSearchStore.type
        override val storeManager: OpenSearchStore.type = OpenSearchStore
        override def name: String = "OpenSearchConfigSpec"
        override lazy val directory: Option[Path] = None
        override def upgrades: List[DatabaseUpgrade] = Nil
      }

      val parentCfg = OpenSearchConfig.from(new DB, "Parent")
      parentCfg.joinDomain shouldBe Some("join_cfg_spec")
      parentCfg.joinRole shouldBe Some("parent")
      parentCfg.joinChildren shouldBe List("Child")

      // restore
      prev.foreach {
        case (k, Some(v)) => Profig(k).store(v)
        case (k, None) => Profig(k).remove()
      }
    }

    "fail inference when a parent claims a child but no joinChildParentFields mapping is provided" in {
      val prev = List(
        "lightdb.opensearch.baseUrl",
        "lightdb.opensearch.Parent.joinDomain",
        "lightdb.opensearch.Parent.joinChildren",
        "lightdb.opensearch.Parent.joinChildParentFields",
        "lightdb.opensearch.Child.joinDomain",
        "lightdb.opensearch.Child.joinRole",
        "lightdb.opensearch.Child.joinParentField"
      ).map(k => k -> Profig(k).opt[String]).toMap

      Profig("lightdb.opensearch.baseUrl").store("http://localhost:9200")
      Profig("lightdb.opensearch.Parent.joinDomain").store("join_cfg_spec")
      Profig("lightdb.opensearch.Parent.joinChildren").store("Child")
      Profig("lightdb.opensearch.Parent.joinChildParentFields").remove()
      Profig("lightdb.opensearch.Child.joinDomain").remove()
      Profig("lightdb.opensearch.Child.joinRole").remove()
      Profig("lightdb.opensearch.Child.joinParentField").remove()

      class DB extends LightDB {
        override type SM = OpenSearchStore.type
        override val storeManager: OpenSearchStore.type = OpenSearchStore
        override def name: String = "OpenSearchConfigSpec"
        override lazy val directory: Option[Path] = None
        override def upgrades: List[DatabaseUpgrade] = Nil
      }

      val e = intercept[IllegalArgumentException] {
        OpenSearchConfig.from(new DB, "Child")
      }
      e.getMessage should include("joinChildParentFields")

      // restore
      prev.foreach {
        case (k, Some(v)) => Profig(k).store(v)
        case (k, None) => Profig(k).remove()
      }
    }

    "fail inference when multiple parents claim the same child (ambiguous)" in {
      val prev = List(
        "lightdb.opensearch.baseUrl",
        "lightdb.opensearch.ParentA.joinDomain",
        "lightdb.opensearch.ParentA.joinChildren",
        "lightdb.opensearch.ParentA.joinChildParentFields",
        "lightdb.opensearch.ParentB.joinDomain",
        "lightdb.opensearch.ParentB.joinChildren",
        "lightdb.opensearch.ParentB.joinChildParentFields",
        "lightdb.opensearch.Child.joinDomain",
        "lightdb.opensearch.Child.joinRole",
        "lightdb.opensearch.Child.joinParentField"
      ).map(k => k -> Profig(k).opt[String]).toMap

      Profig("lightdb.opensearch.baseUrl").store("http://localhost:9200")
      Profig("lightdb.opensearch.ParentA.joinDomain").store("join_cfg_a")
      Profig("lightdb.opensearch.ParentA.joinChildren").store("Child")
      Profig("lightdb.opensearch.ParentA.joinChildParentFields").store("Child:parentId")

      Profig("lightdb.opensearch.ParentB.joinDomain").store("join_cfg_b")
      Profig("lightdb.opensearch.ParentB.joinChildren").store("Child")
      Profig("lightdb.opensearch.ParentB.joinChildParentFields").store("Child:parentId")

      Profig("lightdb.opensearch.Child.joinDomain").remove()
      Profig("lightdb.opensearch.Child.joinRole").remove()
      Profig("lightdb.opensearch.Child.joinParentField").remove()

      class DB extends LightDB {
        override type SM = OpenSearchStore.type
        override val storeManager: OpenSearchStore.type = OpenSearchStore
        override def name: String = "OpenSearchConfigSpec"
        override lazy val directory: Option[Path] = None
        override def upgrades: List[DatabaseUpgrade] = Nil
      }

      val e = intercept[IllegalArgumentException] {
        OpenSearchConfig.from(new DB, "Child")
      }
      e.getMessage should include("ambiguous")

      // restore
      prev.foreach {
        case (k, Some(v)) => Profig(k).store(v)
        case (k, None) => Profig(k).remove()
      }
    }

    "support programmatic join-domain registration (no Profig keys) via OpenSearchJoinDomainRegistry" in {
      val prev = List(
        "lightdb.opensearch.baseUrl",
        "lightdb.opensearch.Parent.joinDomain",
        "lightdb.opensearch.Parent.joinRole",
        "lightdb.opensearch.Parent.joinChildren",
        "lightdb.opensearch.Parent.joinChildParentFields",
        "lightdb.opensearch.Child.joinDomain",
        "lightdb.opensearch.Child.joinRole",
        "lightdb.opensearch.Child.joinParentField",
        "lightdb.opensearch.joinFieldName"
      ).map(k => k -> Profig(k).opt[String]).toMap

      Profig("lightdb.opensearch.baseUrl").store("http://localhost:9200")
      Profig("lightdb.opensearch.Parent.joinDomain").remove()
      Profig("lightdb.opensearch.Parent.joinRole").remove()
      Profig("lightdb.opensearch.Parent.joinChildren").remove()
      Profig("lightdb.opensearch.Parent.joinChildParentFields").remove()
      Profig("lightdb.opensearch.Child.joinDomain").remove()
      Profig("lightdb.opensearch.Child.joinRole").remove()
      Profig("lightdb.opensearch.Child.joinParentField").remove()
      Profig("lightdb.opensearch.joinFieldName").remove()

      class DB extends LightDB {
        override type SM = OpenSearchStore.type
        override val storeManager: OpenSearchStore.type = OpenSearchStore
        override def name: String = "OpenSearchConfigRegistrySpec"
        override lazy val directory: Option[Path] = None
        override def upgrades: List[DatabaseUpgrade] = Nil
      }
      val db = new DB

      OpenSearchJoinDomainRegistry.withRegistration(
        dbName = db.name,
        joinDomain = "reg_domain",
        parentStoreName = "Parent",
        childJoinParentFields = Map("Child" -> "parentId"),
        joinFieldName = "__lightdb_join"
      ) {
        val parentCfg = OpenSearchConfig.from(db, "Parent")
        parentCfg.joinDomain shouldBe Some("reg_domain")
        parentCfg.joinRole shouldBe Some("parent")
        parentCfg.joinChildren shouldBe List("Child")

        val childCfg = OpenSearchConfig.from(db, "Child")
        childCfg.joinDomain shouldBe Some("reg_domain")
        childCfg.joinRole shouldBe Some("child")
        childCfg.joinParentField shouldBe Some("parentId")
      }

      // restore
      prev.foreach {
        case (k, Some(v)) => Profig(k).store(v)
        case (k, None) => Profig(k).remove()
      }
    }
  }
}


