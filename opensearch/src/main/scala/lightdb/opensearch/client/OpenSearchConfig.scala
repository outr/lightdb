package lightdb.opensearch.client

import fabric.Null
import fabric.rw._
import lightdb.LightDB
import profig.Profig

import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}

case class OpenSearchConfig(baseUrl: String,
                            indexPrefix: Option[String] = None,
                            requestTimeout: FiniteDuration = 10.seconds,
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
                            maxResultWindow: Int = 250_000,
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
                            bulkMaxDocs: Int = 5_000,
                            bulkMaxBytes: Int = 5 * 1024 * 1024,
                            joinDomain: Option[String] = None,
                            joinRole: Option[String] = None,
                            joinChildren: List[String] = Nil,
                            joinParentField: Option[String] = None,
                            joinScoreMode: String = "none",
                            joinFieldName: String = "__lightdb_join") {
  lazy val normalizedBaseUrl: String = if (baseUrl.endsWith("/")) baseUrl.dropRight(1) else baseUrl
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
    val baseUrl = sys.props
      .get("lightdb.opensearch.baseUrl")
      .orElse(Profig("lightdb.opensearch.baseUrl").opt[String])
      .getOrElse(throw new RuntimeException("Missing config: lightdb.opensearch.baseUrl"))
    val indexPrefix = sys.props
      .get("lightdb.opensearch.indexPrefix")
      .orElse(Profig("lightdb.opensearch.indexPrefix").opt[String])
    val refreshPolicy = sys.props
      .get("lightdb.opensearch.refreshPolicy")
      .orElse(Profig("lightdb.opensearch.refreshPolicy").opt[String])

    val opaqueId = sys.props
      .get("lightdb.opensearch.opaqueId")
      .orElse(Profig("lightdb.opensearch.opaqueId").opt[String])
      .map(_.trim)
      .filter(_.nonEmpty)
    val logRequests = sys.props
      .get("lightdb.opensearch.logRequests")
      .orElse(Profig("lightdb.opensearch.logRequests").opt[String])
      .exists(_.trim.equalsIgnoreCase("true"))

    val maxResultWindow = sys.props
      .get("lightdb.opensearch.maxResultWindow")
      .orElse(Profig("lightdb.opensearch.maxResultWindow").opt[String])
      .flatMap(s => scala.util.Try(s.toInt).toOption)
      .filter(_ >= 1)
      .getOrElse(250_000)

    val trackTotalHitsUpTo = sys.props
      .get("lightdb.opensearch.trackTotalHitsUpTo")
      .orElse(Profig("lightdb.opensearch.trackTotalHitsUpTo").opt[String])
      .flatMap(s => scala.util.Try(s.toInt).toOption)
      .filter(_ >= 1)

    val useIndexAlias = sys.props
      .get("lightdb.opensearch.useIndexAlias")
      .orElse(Profig("lightdb.opensearch.useIndexAlias").opt[String])
      .exists(s => s.equalsIgnoreCase("true"))
    val indexAliasSuffix = sys.props
      .get("lightdb.opensearch.indexAliasSuffix")
      .orElse(Profig("lightdb.opensearch.indexAliasSuffix").opt[String])
      .getOrElse("_000001")
    val useWriteAlias = sys.props
      .get("lightdb.opensearch.useWriteAlias")
      .orElse(Profig("lightdb.opensearch.useWriteAlias").opt[String])
      .exists(s => s.equalsIgnoreCase("true"))
    val writeAliasSuffix = sys.props
      .get("lightdb.opensearch.writeAliasSuffix")
      .orElse(Profig("lightdb.opensearch.writeAliasSuffix").opt[String])
      .getOrElse("_write")

    // Auth:
    // Prefer a prebuilt Authorization header if provided, otherwise construct from specific config keys.
    val authHeader = sys.props
      .get("lightdb.opensearch.authHeader")
      .orElse(Profig("lightdb.opensearch.authHeader").opt[String])
      .orElse {
        val user = sys.props.get("lightdb.opensearch.username").orElse(Profig("lightdb.opensearch.username").opt[String])
        val pass = sys.props.get("lightdb.opensearch.password").orElse(Profig("lightdb.opensearch.password").opt[String])
        (user, pass) match {
          case (Some(u), Some(p)) =>
            val raw = s"$u:$p"
            val encoded = java.util.Base64.getEncoder.encodeToString(raw.getBytes("UTF-8"))
            Some(s"Basic $encoded")
          case _ =>
            sys.props.get("lightdb.opensearch.bearerToken")
              .orElse(Profig("lightdb.opensearch.bearerToken").opt[String])
              .map(t => s"Bearer $t")
              .orElse {
                sys.props.get("lightdb.opensearch.apiKey")
                  .orElse(Profig("lightdb.opensearch.apiKey").opt[String])
                  .map(k => s"ApiKey $k")
              }
        }
      }

    // Retry/backoff controls
    val retryMaxAttempts = sys.props
      .get("lightdb.opensearch.retry.maxAttempts")
      .orElse(Profig("lightdb.opensearch.retry.maxAttempts").opt[String])
      .flatMap(s => scala.util.Try(s.toInt).toOption)
      .filter(_ >= 1)
      .getOrElse(3)
    val retryInitialDelayMillis = sys.props
      .get("lightdb.opensearch.retry.initialDelayMillis")
      .orElse(Profig("lightdb.opensearch.retry.initialDelayMillis").opt[String])
      .flatMap(s => scala.util.Try(s.toLong).toOption)
      .filter(_ >= 0L)
      .getOrElse(200L)
    val retryMaxDelayMillis = sys.props
      .get("lightdb.opensearch.retry.maxDelayMillis")
      .orElse(Profig("lightdb.opensearch.retry.maxDelayMillis").opt[String])
      .flatMap(s => scala.util.Try(s.toLong).toOption)
      .filter(_ >= 0L)
      .getOrElse(5000L)

    // Bulk sizing controls
    val bulkMaxDocs = sys.props
      .get("lightdb.opensearch.bulk.maxDocs")
      .orElse(Profig("lightdb.opensearch.bulk.maxDocs").opt[String])
      .flatMap(s => scala.util.Try(s.toInt).toOption)
      .filter(_ >= 1)
      .getOrElse(5000)
    val bulkMaxBytes = sys.props
      .get("lightdb.opensearch.bulk.maxBytes")
      .orElse(Profig("lightdb.opensearch.bulk.maxBytes").opt[String])
      .flatMap(s => scala.util.Try(s.toInt).toOption)
      .filter(_ >= 1)
      .getOrElse(5 * 1024 * 1024)

    val joinDomain = sys.props
      .get(s"lightdb.opensearch.$collectionName.joinDomain")
      .orElse(Profig(s"lightdb.opensearch.$collectionName.joinDomain").opt[String])
    val joinRole = sys.props
      .get(s"lightdb.opensearch.$collectionName.joinRole")
      .orElse(Profig(s"lightdb.opensearch.$collectionName.joinRole").opt[String])
    val joinChildren = sys.props
      .get(s"lightdb.opensearch.$collectionName.joinChildren")
      .orElse(Profig(s"lightdb.opensearch.$collectionName.joinChildren").opt[String])
      .map(_.split(",").toList.map(_.trim).filter(_.nonEmpty))
      .getOrElse(Nil)
    val joinParentField = sys.props
      .get(s"lightdb.opensearch.$collectionName.joinParentField")
      .orElse(Profig(s"lightdb.opensearch.$collectionName.joinParentField").opt[String])
    val joinFieldName = sys.props
      .get("lightdb.opensearch.joinFieldName")
      .orElse(Profig("lightdb.opensearch.joinFieldName").opt[String])
      .getOrElse("__lightdb_join")
    val joinScoreMode = sys.props
      .get(s"lightdb.opensearch.$collectionName.joinScoreMode")
      .orElse(Profig(s"lightdb.opensearch.$collectionName.joinScoreMode").opt[String])
      .orElse(sys.props.get("lightdb.opensearch.joinScoreMode"))
      .orElse(Profig("lightdb.opensearch.joinScoreMode").opt[String])
      .map(_.trim.toLowerCase)
      .filter(s => Set("none", "max", "sum", "avg", "min").contains(s))
      .getOrElse("none")
    val cfg = OpenSearchConfig(
      baseUrl = baseUrl,
      indexPrefix = indexPrefix,
      refreshPolicy = refreshPolicy,
      authHeader = authHeader,
      opaqueId = opaqueId,
      logRequests = logRequests,
      maxResultWindow = maxResultWindow,
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
      joinDomain = joinDomain,
      joinRole = joinRole,
      joinChildren = joinChildren,
      joinParentField = joinParentField,
      joinScoreMode = joinScoreMode,
      joinFieldName = joinFieldName
    )
    if (cfg.logRequests) {
      scribe.info(s"OpenSearchConfig(baseUrl=${cfg.baseUrl}, indexPrefix=${cfg.indexPrefix.getOrElse("")}, timeout=${cfg.requestTimeout}, refresh=${cfg.refreshPolicy.getOrElse("")})")
    }
    cfg
  }
}


