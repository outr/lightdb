package lightdb.opensearch.client

import fabric.Null
import fabric.rw.*
import lightdb.LightDB
import lightdb.opensearch.OpenSearchJoinDomainRegistry
import profig.Profig

import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}

case class OpenSearchConfig(baseUrl: String,
                            indexPrefix: Option[String] = None,
                            requestTimeout: FiniteDuration = 10.seconds,
                            /**
                             * If set, logs when an HTTP request has been in-flight longer than this duration.
                             *
                             * This is useful to distinguish "the system is stuck" from "we're waiting on a slow OpenSearch call".
                             *
                             * Set to None to disable.
                             */
                            slowRequestLogAfter: Option[FiniteDuration] = Some(60.seconds),
                            /**
                             * If `slowRequestLogAfter` is enabled, optionally re-log at this interval while the request is still running.
                             * Set to None to only log once at `slowRequestLogAfter`.
                             */
                            slowRequestLogEvery: Option[FiniteDuration] = Some(60.seconds),
                            refreshPolicy: Option[String] = None,
                            authHeader: Option[String] = None,
                            /**
                             * If provided, this value will be sent as OpenSearch `X-Opaque-Id` on every request.
                             * This enables request correlation in OpenSearch logs.
                             */
                            opaqueId: Option[String] = None,
                            /**
                             * When enabled, logs OpenSearch requests (method, path, status, duration) including retries.
                             */
                            logRequests: Boolean = false,
                            /**
                             * Optional list of URL path fragments to exclude from request logging when logRequests=true.
                             * Useful to avoid spamming logs with high-volume endpoints like `/_bulk`.
                             */
                            logRequestsExcludePaths: List[String] = Nil,
                            maxResultWindow: Int = 250_000,
                            /**
                             * Optional OpenSearch index sorting configuration.
                             *
                             * When configured, LightDB will emit `index.sort.field` / `index.sort.order` in the index settings
                             * at index creation time. (Changing this requires creating a new index.)
                             */
                            indexSortFields: List[String] = Nil,
                            indexSortOrders: List[String] = Nil,
                            /**
                             * If set, OpenSearch will compute exact totals only up to this threshold. Above it, totals become "gte".
                             * This is a useful production knob to avoid expensive exact hit counts.
                             *
                             * Only applies when Query.countTotal=true.
                             */
                            trackTotalHitsUpTo: Option[Int] = None,
                            useIndexAlias: Boolean = false,
                            indexAliasSuffix: String = "_000001",
                            useWriteAlias: Boolean = false,
                            writeAliasSuffix: String = "_write",
                            retryMaxAttempts: Int = 3,
                            retryInitialDelay: FiniteDuration = 200.millis,
                            retryMaxDelay: FiniteDuration = 5.seconds,
                            retryStatusCodes: Set[Int] = Set(429, 502, 503, 504),
                            /**
                             * Global ingest concurrency limiter (shared across stores using the same baseUrl).
                             * When set, OpenSearch commit writes will acquire a permit before sending requests.
                             * Defaults to unlimited.
                             */
                            ingestMaxConcurrentRequests: Int = Int.MaxValue,
                            /**
                             * When enabled, OpenSearch requests will emit in-process metrics (counters + timing).
                             * This is intentionally lightweight and dependency-free; wire to an external sink as needed.
                             */
                            metricsEnabled: Boolean = false,
                            /**
                             * If set (and metricsEnabled=true), periodically logs a snapshot of metrics for this baseUrl.
                             */
                            metricsLogEvery: Option[FiniteDuration] = None,
                            /**
                             * If set, this value will be sent as OpenSearch `filter_path` on search requests to reduce response payload size.
                             *
                             * Example: `hits.hits._id,hits.hits._source,hits.hits._score,hits.hits.sort,hits.total.value,aggregations`
                             */
                            searchFilterPath: Option[String] = None,
                            bulkMaxDocs: Int = 50_000,
                            bulkMaxBytes: Int = 5 * 1024 * 1024,
                            /**
                             * Maximum number of concurrent bulk requests issued during a commit when the buffered
                             * operations are chunked. Defaults to 1 (sequential).
                             */
                            bulkConcurrency: Int = 1,
                            /**
                             * When true, facet aggregations will include a synthetic bucket for documents that have
                             * no values for a facet field. This is implemented by indexing a special token in the
                             * `<field>__facet` field when the facet value list is empty.
                             */
                            facetIncludeMissing: Boolean = false,
                            /**
                             * OpenSearch facet implementation detail:
                             *
                             * LightDB needs Lucene-like facet semantics where:
                             * - FacetResult.values can be limited (top-N)
                             * - FacetResult.childCount reflects the TOTAL number of distinct children
                             *
                             * OpenSearch `terms` aggregations do not return the total number of distinct buckets.
                             *
                             * Default behavior:
                             * - Use `cardinality` aggregation for fast (approximate) `childCount`.
                             * - Allow opting into exact totals via composite aggregation paging when required.
                             *
                             * This setting acts as a safety cap for any facet aggregation that needs to fetch "many"
                             * buckets (e.g. childrenLimit=None). It should generally be kept <= `search.max_buckets`.
                             */
                            facetAggMaxBuckets: Int = 65_536,
                            /**
                             * Controls how OpenSearch computes FacetResult.childCount.
                             *
                             * - "cardinality" (default): fast approximate distinct count (one request).
                             * - "composite": exact distinct count by paging composite aggs (many requests for high-cardinality facets).
                             *
                             * Note: hierarchical facet paths may force composite mode for correctness.
                             */
                            facetChildCountMode: String = "cardinality",
                            /**
                             * Optional OpenSearch cardinality precision threshold (1..40000). Higher values improve accuracy
                             * at the cost of memory on the OpenSearch node. Only used when facetChildCountMode=cardinality.
                             */
                            facetChildCountPrecisionThreshold: Option[Int] = None,
                            /**
                             * Page size for composite aggregations used to compute exact facet `childCount`.
                             * Keep this well below `search.max_buckets` (default 65535).
                             */
                            facetChildCountPageSize: Int = 1000,
                            /**
                             * Safety cap: maximum number of composite pages to fetch when computing a facet `childCount`.
                             * Prevents unbounded looping on extremely high-cardinality facets.
                             */
                            facetChildCountMaxPages: Int = 10_000,
                            /**
                             * When true, skip mapping-hash verification during store initialization.
                             * This is primarily an escape hatch for existing indices created before mapping hashes existed.
                             */
                            ignoreMappingHash: Boolean = false,
                            /**
                             * When try, only warns when mapping-hash verification fails. Defaults to true.
                             */
                            mappingHashWarnOnly: Boolean = true,
                            /**
                             * When true (and useIndexAlias=true), automatically migrates mapping hash mismatches by:
                             * - creating a new physical index (next generation)
                             * - reindexing documents from the read alias into it
                             * - atomically repointing read/write aliases
                             *
                             * This is intended for "derived index" usage (e.g. SplitCollection search backends) to provide
                             * self-healing similar to SplitCollection's rebuild behavior.
                             *
                             * WARNING: This does not coordinate concurrent writes. For correctness under concurrent writes,
                             * pause writes or perform a follow-up catch-up reindex before swapping.
                             */
                            mappingHashAutoMigrate: Boolean = false,
                            /**
                             * When enabled, bulk indexing failures will be captured into a dedicated dead-letter index
                             * (best-effort) before the transaction fails.
                             */
                            deadLetterEnabled: Boolean = false,
                            /**
                             * Suffix appended to the derived dead-letter index name.
                             */
                            deadLetterIndexSuffix: String = "_deadletter",
                            /**
                             * Optional keyword-field normalization. When enabled, LightDB will:
                             * - create keyword subfields with a normalizer (trim + lowercase)
                             * - normalize query literals for keyword-field operations (term/terms/prefix on `.keyword`)
                             *
                             * Default is false to preserve Lucene's case-sensitive exact matching semantics for non-tokenized fields.
                             */
                            keywordNormalize: Boolean = false,
                            /**
                             * When true, LightDB is allowed to emit OpenSearch `_script` sorts as a compatibility fallback
                             * (e.g. for legacy indices missing `.keyword` subfields).
                             *
                             * WARNING: Script sorts can be extremely slow on large result sets and should generally be
                             * avoided in production. Prefer fixing mappings / rebuilding indices instead.
                             *
                             * Default is false: never emit script sorts.
                             */
                            allowScriptSorts: Boolean = false,
                            joinDomain: Option[String] = None,
                            joinRole: Option[String] = None,
                            joinChildren: List[String] = Nil,
                            joinParentField: Option[String] = None,
                            joinScoreMode: String = "none",
                            joinFieldName: String = "__lightdb_join") {
  lazy val normalizedBaseUrl: String = if baseUrl.endsWith("/") then baseUrl.dropRight(1) else baseUrl
}

object OpenSearchConfig {
  /**
   * Reads configuration from Profig, scoped by db and collection name where useful.
   *
   * Keys (initial):
   * - lightdb.opensearch.baseUrl
   * - lightdb.opensearch.indexPrefix
   * - lightdb.opensearch.refreshPolicy
   */
  def from(db: LightDB, collectionName: String): OpenSearchConfig = {
    def optString(key: String): Option[String] =
      Profig(key).opt[String].map(_.trim).filter(_.nonEmpty)

    def optBoolean(key: String): Option[Boolean] =
      Profig(key).opt[Boolean]

    def optInt(key: String): Option[Int] =
      Profig(key).opt[Int]

    def optLong(key: String): Option[Long] =
      Profig(key).opt[Long]

    def parseCsv(s: String): List[String] = s
      .split(",")
      .toList
      .map(_.trim)
      .filter(_.nonEmpty)

    def optStringList(key: String): List[String] =
      Profig(key)
        .opt[List[String]]
        .getOrElse(optString(key).map(parseCsv).getOrElse(Nil))
        .map(_.trim)
        .filter(_.nonEmpty)

    val baseUrl = optString("lightdb.opensearch.baseUrl")
      .getOrElse(throw new RuntimeException("Missing config: lightdb.opensearch.baseUrl"))

    val requestTimeoutMillis = optLong(s"lightdb.opensearch.$collectionName.requestTimeoutMillis")
      .orElse(optLong("lightdb.opensearch.requestTimeoutMillis"))
      .filter(_ > 0L)
      .getOrElse(10.seconds.toMillis)

    val slowRequestLogAfterMillis =
      optLong(s"lightdb.opensearch.$collectionName.slowRequest.logAfterMillis")
        .orElse(optLong("lightdb.opensearch.slowRequest.logAfterMillis"))
        .filter(_ > 0L)
    val slowRequestLogEveryMillis =
      optLong(s"lightdb.opensearch.$collectionName.slowRequest.logEveryMillis")
        .orElse(optLong("lightdb.opensearch.slowRequest.logEveryMillis"))
        .filter(_ > 0L)

    val slowRequestLogAfter = slowRequestLogAfterMillis.map(_.millis).orElse(Some(60.seconds))
    val slowRequestLogEvery = slowRequestLogEveryMillis.map(_.millis).orElse(Some(60.seconds))
    val indexPrefix = optString("lightdb.opensearch.indexPrefix")
    // Default to `false` for maximum ingest throughput. LightDB's OpenSearchTransaction will explicitly refresh
    // at commit boundaries, so documents become searchable after the transaction completes without paying a
    // per-bulk "wait_for" cost.
    val refreshPolicy = optString(s"lightdb.opensearch.$collectionName.refreshPolicy")
      .orElse(optString("lightdb.opensearch.refreshPolicy"))
      .orElse(Some("false"))

    val opaqueId = optString("lightdb.opensearch.opaqueId")
    val logRequests = optBoolean("lightdb.opensearch.logRequests").getOrElse(false)
    val logRequestsExcludePaths =
      Profig("lightdb.opensearch.logRequestsExcludePaths")
        .opt[List[String]]
        .getOrElse(Nil)
        .map(_.trim)
        .filter(_.nonEmpty)

    val maxResultWindow = optInt("lightdb.opensearch.maxResultWindow")
      .filter(_ >= 1)
      .getOrElse(250_000)

    val indexSortFields = optStringList(s"lightdb.opensearch.$collectionName.index.sort.fields") match {
      case Nil => optStringList("lightdb.opensearch.index.sort.fields")
      case list => list
    }
    val indexSortOrders = optStringList(s"lightdb.opensearch.$collectionName.index.sort.orders") match {
      case Nil => optStringList("lightdb.opensearch.index.sort.orders")
      case list => list
    }

    val trackTotalHitsUpTo = optInt("lightdb.opensearch.trackTotalHitsUpTo")
      .filter(_ >= 1)

    val useIndexAlias = optBoolean(s"lightdb.opensearch.$collectionName.useIndexAlias")
      .orElse(optBoolean("lightdb.opensearch.useIndexAlias"))
      .getOrElse(false)
    val indexAliasSuffix = optString(s"lightdb.opensearch.$collectionName.indexAliasSuffix")
      .orElse(optString("lightdb.opensearch.indexAliasSuffix"))
      .getOrElse("_000001")
    val useWriteAlias = optBoolean(s"lightdb.opensearch.$collectionName.useWriteAlias")
      .orElse(optBoolean("lightdb.opensearch.useWriteAlias"))
      .getOrElse(false)
    val writeAliasSuffix = optString(s"lightdb.opensearch.$collectionName.writeAliasSuffix")
      .orElse(optString("lightdb.opensearch.writeAliasSuffix"))
      .getOrElse("_write")

    // Auth:
    // Prefer a prebuilt Authorization header if provided, otherwise construct from specific config keys.
    val authHeader = optString("lightdb.opensearch.authHeader")
      .orElse {
        val user = optString("lightdb.opensearch.username")
        val pass = optString("lightdb.opensearch.password")
        (user, pass) match {
          case (Some(u), Some(p)) =>
            val raw = s"$u:$p"
            val encoded = java.util.Base64.getEncoder.encodeToString(raw.getBytes("UTF-8"))
            Some(s"Basic $encoded")
          case _ =>
            optString("lightdb.opensearch.bearerToken")
              .map(t => s"Bearer $t")
              .orElse {
                optString("lightdb.opensearch.apiKey")
                  .map(k => s"ApiKey $k")
              }
        }
      }

    // Retry/backoff controls
    val retryMaxAttempts = optInt("lightdb.opensearch.retry.maxAttempts")
      .filter(_ >= 1)
      .getOrElse(3)
    val retryInitialDelayMillis = optLong("lightdb.opensearch.retry.initialDelayMillis")
      .filter(_ >= 0L)
      .getOrElse(200L)
    val retryMaxDelayMillis = optLong("lightdb.opensearch.retry.maxDelayMillis")
      .filter(_ >= 0L)
      .getOrElse(5000L)

    val ingestMaxConcurrentRequests = optInt("lightdb.opensearch.ingest.maxConcurrentRequests")
      .filter(_ >= 1)
      .getOrElse(Int.MaxValue)

    val metricsEnabled = optBoolean("lightdb.opensearch.metrics.enabled").getOrElse(false)

    val metricsLogEvery = optLong("lightdb.opensearch.metrics.logEveryMillis")
      .filter(_ > 0L)
      .map(_.millis)

    val searchFilterPath = optString(s"lightdb.opensearch.$collectionName.search.filterPath")
      .orElse(optString("lightdb.opensearch.search.filterPath"))

    // Facet tuning (search-time)
    val facetAggMaxBuckets = optInt(s"lightdb.opensearch.$collectionName.facetAggMaxBuckets")
      .orElse(optInt("lightdb.opensearch.facetAggMaxBuckets"))
      .filter(_ >= 1)
      .getOrElse(65_536)

    val facetChildCountMode = optString(s"lightdb.opensearch.$collectionName.facetChildCount.mode")
      .orElse(optString("lightdb.opensearch.facetChildCount.mode"))
      .map(_.trim.toLowerCase)
      .filter(s => Set("cardinality", "composite").contains(s))
      .getOrElse("cardinality")

    val facetChildCountPrecisionThreshold = optInt(s"lightdb.opensearch.$collectionName.facetChildCount.precisionThreshold")
      .orElse(optInt("lightdb.opensearch.facetChildCount.precisionThreshold"))
      .filter(v => v >= 1 && v <= 40_000)

    val facetChildCountPageSize = optInt(s"lightdb.opensearch.$collectionName.facetChildCount.pageSize")
      .orElse(optInt("lightdb.opensearch.facetChildCount.pageSize"))
      .orElse(optInt(s"lightdb.opensearch.$collectionName.facetChildCountPageSize"))
      .orElse(optInt("lightdb.opensearch.facetChildCountPageSize"))
      .filter(_ >= 1)
      .getOrElse(1000)

    val facetChildCountMaxPages = optInt(s"lightdb.opensearch.$collectionName.facetChildCount.maxPages")
      .orElse(optInt("lightdb.opensearch.facetChildCount.maxPages"))
      .orElse(optInt(s"lightdb.opensearch.$collectionName.facetChildCountMaxPages"))
      .orElse(optInt("lightdb.opensearch.facetChildCountMaxPages"))
      .filter(_ >= 1)
      .getOrElse(10_000)

    // Bulk sizing controls
    val bulkMaxDocs = optInt("lightdb.opensearch.bulk.maxDocs")
      .filter(_ >= 1)
      .getOrElse(5000)
    val bulkMaxBytes = optInt("lightdb.opensearch.bulk.maxBytes")
      .filter(_ >= 1)
      .getOrElse(5 * 1024 * 1024)

    val bulkConcurrency = optInt("lightdb.opensearch.bulk.concurrency")
      .filter(_ >= 1)
      .getOrElse(1)

    val facetIncludeMissing = optBoolean(s"lightdb.opensearch.$collectionName.facets.includeMissing")
      .orElse(optBoolean("lightdb.opensearch.facets.includeMissing"))
      .getOrElse(false)

    val ignoreMappingHash = optBoolean("lightdb.opensearch.ignoreMappingHash").getOrElse(false)
    val mappingHashWarnOnly = optBoolean(s"lightdb.opensearch.$collectionName.mappingHash.warnOnly")
      .orElse(optBoolean("lightdb.opensearch.mappingHash.warnOnly"))
      .getOrElse(true)
    val mappingHashAutoMigrate = optBoolean(s"lightdb.opensearch.$collectionName.mappingHash.autoMigrate")
      .orElse(optBoolean("lightdb.opensearch.mappingHash.autoMigrate"))
      .getOrElse(false)

    val deadLetterEnabled = optBoolean(s"lightdb.opensearch.$collectionName.deadLetter.enabled")
      .orElse(optBoolean("lightdb.opensearch.deadLetter.enabled"))
      .getOrElse(false)

    val deadLetterIndexSuffix = optString("lightdb.opensearch.deadLetter.indexSuffix")
      .getOrElse("_deadletter")

    val keywordNormalize = optBoolean(s"lightdb.opensearch.$collectionName.keyword.normalize")
      .orElse(optBoolean("lightdb.opensearch.keyword.normalize"))
      .getOrElse(false)

    val allowScriptSorts = optBoolean(s"lightdb.opensearch.$collectionName.allowScriptSorts")
      .orElse(optBoolean("lightdb.opensearch.allowScriptSorts"))
      .getOrElse(false)

    def parseChildParentFields(s: String): Map[String, String] = {
      // Format: "ChildStoreA:parentIdField,ChildStoreB:parentIdField"
      parseCsv(s).flatMap { pair =>
        val i = pair.indexOf(':')
        if i <= 0 || i >= pair.length - 1 then {
          None
        } else {
          val child = pair.substring(0, i).trim
          val field = pair.substring(i + 1).trim
          if child.isEmpty || field.isEmpty then None else Some(child -> field)
        }
      }.toMap
    }

    val joinDomainRaw = optString(s"lightdb.opensearch.$collectionName.joinDomain")
    val joinRoleRaw = optString(s"lightdb.opensearch.$collectionName.joinRole")
    val joinChildrenRaw = optString(s"lightdb.opensearch.$collectionName.joinChildren").map(parseCsv).getOrElse(Nil)
    val joinChildParentFieldsRaw: Map[String, String] =
      optString(s"lightdb.opensearch.$collectionName.joinChildParentFields").map(parseChildParentFields).getOrElse(Map.empty)
    val joinParentFieldRaw = optString(s"lightdb.opensearch.$collectionName.joinParentField")

    // Programmatic join-domain config (no JVM system properties): allow apps to register join config in code.
    val registryEntry = OpenSearchJoinDomainRegistry.get(db.name, collectionName)
    val joinDomainRaw2 = joinDomainRaw.orElse(registryEntry.map(_.joinDomain))
    val joinRoleRaw2 = joinRoleRaw.orElse(registryEntry.map(_.joinRole))
    val joinChildrenRaw2 = if joinChildrenRaw.nonEmpty then joinChildrenRaw else registryEntry.map(_.joinChildren).getOrElse(Nil)
    val joinParentFieldRaw2 = joinParentFieldRaw.orElse(registryEntry.flatMap(_.joinParentField))
    // Join-domain inference:
    //
    // To reduce config duplication, child stores may infer join config from a parent store that declares:
    // - lightdb.opensearch.<ParentStore>.joinDomain
    // - lightdb.opensearch.<ParentStore>.joinChildren
    // - lightdb.opensearch.<ParentStore>.joinChildParentFields (map: childStoreName -> parentId field name in child)
    //
    // This is system-properties-driven because Profig does not provide a safe way to enumerate all configured keys.
    case class InferredChild(parentStoreName: String, joinDomain: String, joinParentField: String)

    def extractStoreName(key: String, suffix: String): Option[String] = {
      val prefix = "lightdb.opensearch."
      if key.startsWith(prefix) && key.endsWith(suffix) then {
        Some(key.substring(prefix.length, key.length - suffix.length))
      } else {
        None
      }
    }

    def allProfigKeys(json: fabric.Json, prefix: String = ""): List[String] = json match {
      case o: fabric.Obj =>
        o.value.toList.flatMap { case (k, v) =>
          val full = if prefix.isEmpty then k else s"$prefix.$k"
          full :: allProfigKeys(v, full)
        }
      case _ =>
        Nil
    }

    val candidateParentStores: Set[String] =
      allProfigKeys(Profig.json).flatMap { k =>
        extractStoreName(k, ".joinChildren").orElse(extractStoreName(k, ".joinChildParentFields"))
      }.toSet

    val inferredChild: Option[InferredChild] = if joinDomainRaw2.nonEmpty || joinRoleRaw2.nonEmpty || joinParentFieldRaw2.nonEmpty then {
      None
    } else {
      val candidates = candidateParentStores.toList.flatMap { parentStoreName =>
        val parentJoinChildren = optString(s"lightdb.opensearch.$parentStoreName.joinChildren").map(parseCsv).getOrElse(Nil)
        val parentChildParentFields =
          optString(s"lightdb.opensearch.$parentStoreName.joinChildParentFields").map(parseChildParentFields).getOrElse(Map.empty)
        val mentionedAsChild: Boolean =
          parentJoinChildren.contains(collectionName) || parentChildParentFields.contains(collectionName)
        if !mentionedAsChild then {
          Nil
        } else {
          val joinDomain = optString(s"lightdb.opensearch.$parentStoreName.joinDomain").getOrElse(parentStoreName)
          parentChildParentFields.get(collectionName) match {
            case Some(parentField) =>
              List(InferredChild(parentStoreName = parentStoreName, joinDomain = joinDomain, joinParentField = parentField))
            case None =>
              throw new IllegalArgumentException(
                s"OpenSearch join-domain inference for '$collectionName' found parent store '$parentStoreName', but no parent-field mapping was provided. " +
                  s"Set 'lightdb.opensearch.$parentStoreName.joinChildParentFields' (e.g. '$collectionName:parentId') " +
                  s"or explicitly configure 'lightdb.opensearch.$collectionName.joinParentField'."
              )
          }
        }
      }
      candidates match {
        case Nil => None
        case one :: Nil => Some(one)
        case many =>
          throw new IllegalArgumentException(
            s"OpenSearch join-domain inference for '$collectionName' is ambiguous. Multiple parent stores claim this child: " +
              many.map(_.parentStoreName).distinct.sorted.mkString(", ")
          )
      }
    }

    val joinDomain = joinDomainRaw2.orElse(inferredChild.map(_.joinDomain))

    // If this store declares join children or child-parent field mappings and a joinDomain, infer it is a join-parent.
    val inferredJoinRoleParent: Option[String] =
      if joinRoleRaw2.isEmpty && joinDomain.nonEmpty && (joinChildrenRaw2.nonEmpty || joinChildParentFieldsRaw.nonEmpty) then Some("parent") else None

    val joinRole = joinRoleRaw2
      .orElse(inferredChild.map(_ => "child"))
      .orElse(inferredJoinRoleParent)

    val joinChildren: List[String] =
      if joinChildrenRaw2.nonEmpty then joinChildrenRaw2
      else if joinChildParentFieldsRaw.nonEmpty then joinChildParentFieldsRaw.keys.toList.sorted
      else Nil

    val joinParentField = joinParentFieldRaw2.orElse(inferredChild.map(_.joinParentField))

    val joinFieldName = optString("lightdb.opensearch.joinFieldName")
      .orElse(registryEntry.map(_.joinFieldName))
      .getOrElse("__lightdb_join")
    val joinScoreMode = optString(s"lightdb.opensearch.$collectionName.joinScoreMode")
      .orElse(optString("lightdb.opensearch.joinScoreMode"))
      .map(_.trim.toLowerCase)
      .filter(s => Set("none", "max", "sum", "avg", "min").contains(s))
      .getOrElse("none")
    val cfg = OpenSearchConfig(
      baseUrl = baseUrl,
      indexPrefix = indexPrefix,
      requestTimeout = requestTimeoutMillis.millis,
      slowRequestLogAfter = slowRequestLogAfter,
      slowRequestLogEvery = slowRequestLogEvery,
      refreshPolicy = refreshPolicy,
      authHeader = authHeader,
      opaqueId = opaqueId,
      logRequests = logRequests,
      logRequestsExcludePaths = logRequestsExcludePaths,
      maxResultWindow = maxResultWindow,
      indexSortFields = indexSortFields,
      indexSortOrders = indexSortOrders,
      trackTotalHitsUpTo = trackTotalHitsUpTo,
      useIndexAlias = useIndexAlias,
      indexAliasSuffix = indexAliasSuffix,
      useWriteAlias = useWriteAlias,
      writeAliasSuffix = writeAliasSuffix,
      retryMaxAttempts = retryMaxAttempts,
      retryInitialDelay = retryInitialDelayMillis.millis,
      retryMaxDelay = retryMaxDelayMillis.millis,
      bulkMaxDocs = bulkMaxDocs,
      bulkMaxBytes = bulkMaxBytes,
      ingestMaxConcurrentRequests = ingestMaxConcurrentRequests,
      metricsEnabled = metricsEnabled,
      metricsLogEvery = metricsLogEvery,
      searchFilterPath = searchFilterPath,
      bulkConcurrency = bulkConcurrency,
      facetIncludeMissing = facetIncludeMissing,
      facetAggMaxBuckets = facetAggMaxBuckets,
      facetChildCountMode = facetChildCountMode,
      facetChildCountPrecisionThreshold = facetChildCountPrecisionThreshold,
      facetChildCountPageSize = facetChildCountPageSize,
      facetChildCountMaxPages = facetChildCountMaxPages,
      ignoreMappingHash = ignoreMappingHash,
      mappingHashWarnOnly = mappingHashWarnOnly,
      mappingHashAutoMigrate = mappingHashAutoMigrate,
      deadLetterEnabled = deadLetterEnabled,
      deadLetterIndexSuffix = deadLetterIndexSuffix,
      keywordNormalize = keywordNormalize,
      allowScriptSorts = allowScriptSorts,
      joinDomain = joinDomain,
      joinRole = joinRole,
      joinChildren = joinChildren,
      joinParentField = joinParentField,
      joinScoreMode = joinScoreMode,
      joinFieldName = joinFieldName
    )
    if cfg.logRequests then {
      scribe.info(s"OpenSearchConfig(baseUrl=${cfg.baseUrl}, indexPrefix=${cfg.indexPrefix.getOrElse("")}, timeout=${cfg.requestTimeout}, refresh=${cfg.refreshPolicy.getOrElse("")})")
    }
    cfg
  }
}


