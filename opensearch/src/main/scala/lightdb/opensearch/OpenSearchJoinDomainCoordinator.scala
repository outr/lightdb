package lightdb.opensearch

import lightdb.doc.{Document, ParentChildSupport}
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import fabric.Json
import fabric.rw._
import profig.Profig
import rapid.Task

/**
 * Helper utilities for configuring join-domain (native parent/child) stores without having to manually write
 * all Profig keys.
 *
 * This is intentionally conservative: it only writes configuration keys; it does not attempt to create indices,
 * migrate data, or validate cluster state.
 */
object OpenSearchJoinDomainCoordinator {
  /**
   * Store-name-only join-domain configuration helper.
   *
   * This is the safest API because it does not require model instances or store creation. It is ideal when:
   * - you use custom store names (`store(Model, name=Some("..."))`)
   * - you need to apply join config before any store vals are evaluated
   */
  def configForStoreNames(joinDomain: String,
                          parentStoreName: String,
                          childStoreName: String,
                          childJoinParentFieldName: String,
                          joinFieldName: String = "__lightdb_join"): Map[String, String] = Map(
    s"lightdb.opensearch.$parentStoreName.joinDomain" -> joinDomain,
    s"lightdb.opensearch.$parentStoreName.joinRole" -> "parent",
    s"lightdb.opensearch.$parentStoreName.joinChildren" -> childStoreName,
    s"lightdb.opensearch.$childStoreName.joinDomain" -> joinDomain,
    s"lightdb.opensearch.$childStoreName.joinRole" -> "child",
    s"lightdb.opensearch.$childStoreName.joinParentField" -> childJoinParentFieldName,
    "lightdb.opensearch.joinFieldName" -> joinFieldName
  )

  /**
   * Parent-only join-domain configuration for multiple children.
   *
   * This works with `OpenSearchConfig.from` inference, so you do NOT need to set any child-side join keys.
   *
   * @param childJoinParentFields map: childStoreName -> childFieldNameContainingParentId
   */
  def configForStoreNamesParentOnly(joinDomain: String,
                                    parentStoreName: String,
                                    childJoinParentFields: Map[String, String],
                                    joinFieldName: String = "__lightdb_join"): Map[String, String] = {
    val children = childJoinParentFields.keys.toList.sorted
    val childrenCsv = children.mkString(",")
    val fieldsCsv = children.flatMap { child =>
      childJoinParentFields.get(child).map(f => s"$child:$f")
    }.mkString(",")

    Map(
      s"lightdb.opensearch.$parentStoreName.joinDomain" -> joinDomain,
      s"lightdb.opensearch.$parentStoreName.joinRole" -> "parent",
      s"lightdb.opensearch.$parentStoreName.joinChildren" -> childrenCsv,
      s"lightdb.opensearch.$parentStoreName.joinChildParentFields" -> fieldsCsv,
      "lightdb.opensearch.joinFieldName" -> joinFieldName
    )
  }

  /**
   * Convenience helper that derives the child store name from the child model's class name (LightDB default store naming).
   *
   * This does NOT touch `parentModel.childStore`, so it's safe to call before stores are created.
   *
   * If you override the child store name (via `store(..., name=Some("..."))`), use the explicit [[configFor]] overload
   * that accepts `childStoreName`.
   */
  def configForDerivedChildStoreName[Parent <: Document[Parent], Child <: Document[Child], ChildModel <: lightdb.doc.DocumentModel[Child]](
    joinDomain: String,
    parentStoreName: String,
    parentModel: ParentChildSupport[Parent, Child, ChildModel],
    childModel: ChildModel,
    joinFieldName: String = "__lightdb_join"
  ): Map[String, String] = {
    val childStoreName = defaultStoreName(childModel)
    configFor(
      joinDomain = joinDomain,
      parentStoreName = parentStoreName,
      parentModel = parentModel,
      childStoreName = childStoreName,
      childModel = childModel,
      joinFieldName = joinFieldName
    )
  }

  /**
   * Returns the Profig keys needed to configure a join-domain for a single parent/child relation.
   *
   * You can apply the returned map via [[applySysProps]] before constructing your DB/stores (recommended),
   * so `OpenSearchConfig.from` sees the configuration during store creation.
   */
  def configFor[Parent <: Document[Parent], Child <: Document[Child], ChildModel <: lightdb.doc.DocumentModel[Child]](
    joinDomain: String,
    parentStoreName: String,
    parentModel: ParentChildSupport[Parent, Child, ChildModel],
    childStoreName: String,
    childModel: ChildModel,
    joinFieldName: String = "__lightdb_join"
  ): Map[String, String] = {
    val parentFieldName = parentModel.parentField(childModel).name
    configForStoreNames(
      joinDomain = joinDomain,
      parentStoreName = parentStoreName,
      childStoreName = childStoreName,
      childJoinParentFieldName = parentFieldName,
      joinFieldName = joinFieldName
    )
  }

  private def defaultStoreName(model: AnyRef): String =
    model.getClass.getSimpleName.replace("$", "")

  /**
   * Writes all keys in the provided map into Profig, overwriting existing values.
   */
  def applySysProps(config: Map[String, String]): Unit =
    config.foreach { case (k, v) => Profig(k).store(v) }

  /**
   * Applies the provided config map for the duration of `f`, then restores previous values.
   *
   * This is primarily intended for tests.
   */
  def withSysProps[A](config: Map[String, String])(f: => A): A = {
    val prev = config.keys.map(k => k -> Profig(k).opt[String]).toMap
    applySysProps(config)
    try {
      f
    } finally {
      prev.foreach {
        case (k, Some(v)) => Profig(k).store(v)
        case (k, None) => Profig(k).remove()
      }
    }
  }

  /**
   * Computes the shared join-domain alias names as `OpenSearchStore` would, without requiring a store instance.
   *
   * @return (readAlias, writeAlias)
   */
  def joinDomainAliases(dbName: String, joinDomain: String, config: OpenSearchConfig): (String, Option[String]) = {
    val cfg = config.copy(joinDomain = Some(joinDomain))
    val readAlias = OpenSearchIndexName.default(dbName, collectionName = joinDomain, config = cfg)
    val writeAlias =
      if cfg.useIndexAlias && cfg.useWriteAlias then Some(s"$readAlias${cfg.writeAliasSuffix}")
      else None
    (readAlias, writeAlias)
  }

  /**
   * Join-domain wrapper for [[OpenSearchIndexMigration.createIndexAndRepointAliases]].
   */
  def createIndexAndRepointJoinDomainAliases(client: OpenSearchClient,
                                             dbName: String,
                                             joinDomain: String,
                                             config: OpenSearchConfig,
                                             indexBody: Json,
                                             defaultSuffix: String = "_000001"): Task[String] = {
    val (readAlias, writeAlias) = joinDomainAliases(dbName = dbName, joinDomain = joinDomain, config = config)
    OpenSearchIndexMigration.createIndexAndRepointAliases(
      client = client,
      readAlias = readAlias,
      writeAlias = writeAlias,
      indexBody = indexBody,
      defaultSuffix = defaultSuffix
    )
  }

  /**
   * Join-domain wrapper for [[OpenSearchIndexMigration.reindexAndRepointAliases]].
   *
   * WARNING: This does not coordinate concurrent writes. For correctness under concurrent writes, callers must
   * pause writes or dual-write and perform a follow-up catch-up before swapping.
   */
  def reindexAndRepointJoinDomainAliases(client: OpenSearchClient,
                                         dbName: String,
                                         joinDomain: String,
                                         config: OpenSearchConfig,
                                         newIndexBody: Json,
                                         defaultSuffix: String = "_000001"): Task[String] = {
    val (readAlias, writeAlias) = joinDomainAliases(dbName = dbName, joinDomain = joinDomain, config = config)
    OpenSearchIndexMigration.reindexAndRepointAliases(
      client = client,
      readAlias = readAlias,
      writeAlias = writeAlias,
      newIndexBody = newIndexBody,
      defaultSuffix = defaultSuffix
    )
  }

  /**
   * Join-domain wrapper for [[OpenSearchRebuild.rebuildAndRepointAliasesFromSources]].
   *
   * This is the recommended API when OpenSearch is treated as a derived index and you want to rebuild a join-domain
   * from multiple source-of-truth stores (parents + children).
   *
   * WARNING: This does not coordinate concurrent writes. For correctness under concurrent writes, callers should use
   * a dual-write + catch-up strategy and swap aliases only after catch-up completes.
   */
  def rebuildAndRepointJoinDomainAliasesFromSources(client: OpenSearchClient,
                                                    dbName: String,
                                                    joinDomain: String,
                                                    config: OpenSearchConfig,
                                                    indexBody: Json,
                                                    sources: List[OpenSearchRebuild.RebuildSource],
                                                    defaultSuffix: String = "_000001",
                                                    refreshAfter: Boolean = true): Task[String] = {
    val (readAlias, writeAlias) = joinDomainAliases(dbName = dbName, joinDomain = joinDomain, config = config)
    OpenSearchRebuild.rebuildAndRepointAliasesFromSources(
      client = client,
      readAlias = readAlias,
      writeAlias = writeAlias,
      indexBody = indexBody,
      sources = sources,
      config = config,
      defaultSuffix = defaultSuffix,
      refreshAfter = refreshAfter
    )
  }
}


