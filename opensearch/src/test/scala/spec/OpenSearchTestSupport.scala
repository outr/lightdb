package spec

import fabric.rw._
import profig.Profig
import rapid.Unique

/**
 * Test support for OpenSearch-backed specs.
 *
 * Defaults:
 * - Prefer Testcontainers for ad-hoc OpenSearch startup (set `-Dlightdb.opensearch.useTestcontainers=false` to disable)
 * - Fallback to `http://localhost:9200` if Testcontainers is disabled or Docker is unavailable
 * - Use refreshPolicy `true` for deterministic visibility in tests
 */
trait OpenSearchTestSupport extends ProfigTestSupport { this: org.scalatest.Suite =>
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initOpenSearchTestConfig()
  }

  private def probeOpenSearch(baseUrl: String): Boolean = {
    // Lightweight probe to detect an already-running local OpenSearch node.
    // We avoid instantiating the OpenSearch client here to keep bootstrap simple.
    try {
      val url = new java.net.URI(baseUrl + "/").toURL
      val conn = url.openConnection().asInstanceOf[java.net.HttpURLConnection]
      conn.setRequestMethod("GET")
      conn.setConnectTimeout(300)
      conn.setReadTimeout(500)
      conn.setInstanceFollowRedirects(false)
      val code = conn.getResponseCode
      if (code != 200) return false
      val is = conn.getInputStream
      try {
        val bytes = is.readNBytes(4096)
        val body = new String(bytes, java.nio.charset.StandardCharsets.UTF_8).toLowerCase
        body.contains("opensearch") || body.contains("\"cluster_name\"")
      } finally {
        is.close()
      }
    } catch {
      case _: Throwable => false
    }
  }

  private def initOpenSearchTestConfig(): Unit = {
    // Docker daemon on some hosts enforces a minimum API version. Testcontainers' shaded docker-java reads this from
    // the property key `api.version`.
    if (!Profig("api.version").exists()) {
      Profig("api.version").store("1.44")
    }

    val useTestcontainers: Boolean =
      Profig("lightdb.opensearch.useTestcontainers").opt[Boolean].getOrElse(true)

    val defaultLocal = "http://localhost:9200"
    val currentBaseUrl = Profig("lightdb.opensearch.baseUrl").opt[String].getOrElse(defaultLocal)
    val resolvedBaseUrl: String =
      if (currentBaseUrl == defaultLocal && probeOpenSearch(defaultLocal)) {
        // If a local OpenSearch is already running, use it (and do not start Testcontainers).
        defaultLocal
      } else if (useTestcontainers && currentBaseUrl == defaultLocal) {
        // Prefer Testcontainers unless the user explicitly pointed at a non-local endpoint.
        try {
          OpenSearchTestContainer.baseUrl
        } catch {
          case t: Throwable =>
            // Don't hard-fail compilation/test discovery if Docker isn't available; allow localhost fallback.
            scribe.warn(s"Unable to start OpenSearch via Testcontainers. Falling back to localhost: ${t.getMessage}")
            defaultLocal
        }
      } else {
        currentBaseUrl
      }

    if (!Profig("lightdb.opensearch.baseUrl").exists()) {
      Profig("lightdb.opensearch.baseUrl").store(resolvedBaseUrl)
    }
    // Local OpenSearch instances may be slower than Testcontainers (shared disks, other indices, background merges).
    // Ensure long-running ops like `_delete_by_query` don't trip the default 10s request timeout in tests.
    if (!Profig("lightdb.opensearch.requestTimeoutMillis").exists()) {
      Profig("lightdb.opensearch.requestTimeoutMillis").store(60000L)
    }
    // Use `true` for deterministic visibility in tests. (Some OpenSearch APIs only accept true/false.)
    if (!Profig("lightdb.opensearch.refreshPolicy").exists()) {
      Profig("lightdb.opensearch.refreshPolicy").store("true")
    }
    // Use a per-JVM unique prefix to avoid index/mapping reuse across reruns (and between suites).
    if (!Profig("lightdb.opensearch.indexPrefix").exists()) {
      Profig("lightdb.opensearch.indexPrefix").store(s"lightdb_test_${Unique.sync()}")
    }

    // When running against a persistent local OpenSearch, register ONE JVM shutdown hook to clean up our test indices.
    // We intentionally do NOT delete indices during test execution because suites can share the same JVM and may run
    // concurrently; deleting mid-run can cause 404s in other suites.
    OpenSearchTestSupport.registerShutdownCleanupIfNeeded(resolvedBaseUrl)
  }
}

object OpenSearchTestSupport {
  @volatile private var shutdownHookRegistered: Boolean = false

  def registerShutdownCleanupIfNeeded(resolvedBaseUrl: String): Unit = {
    if (resolvedBaseUrl != "http://localhost:9200") return
    if (shutdownHookRegistered) return
    shutdownHookRegistered = true

    sys.addShutdownHook {
      try {
        val baseUrl = profig.Profig("lightdb.opensearch.baseUrl").as[String]
        val prefix = profig.Profig("lightdb.opensearch.indexPrefix").as[String]
        if (baseUrl != "http://localhost:9200") return

        def http(method: String, pathAndQuery: String): (Int, String) = {
          val url = new java.net.URI(baseUrl + pathAndQuery).toURL
          val conn = url.openConnection().asInstanceOf[java.net.HttpURLConnection]
          conn.setRequestMethod(method)
          conn.setConnectTimeout(1000)
          conn.setReadTimeout(5000)
          conn.setInstanceFollowRedirects(false)
          conn.connect()
          val code = conn.getResponseCode
          val is = if (code >= 200 && code < 400) conn.getInputStream else conn.getErrorStream
          val body = if (is != null) {
            try new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
            finally is.close()
          } else {
            ""
          }
          (code, body)
        }

        // Use _cat indices to list indices; then delete each index by exact name (compatible with destructive_requires_name).
        val (code, body) = http("GET", s"/_cat/indices/${prefix}*?h=index&s=index")
        if (code != 200) return
        val indices = body
          .split("\n")
          .iterator
          .map(_.trim)
          .filter(s => s.nonEmpty && s.startsWith(prefix))
          .toVector

        indices.foreach { index =>
          val (dCode, _) = http("DELETE", s"/$index")
          if (dCode != 200 && dCode != 202 && dCode != 404) {
            // Best-effort; never fail shutdown on cleanup.
            scribe.warn(s"Unable to delete OpenSearch test index '$index' (status=$dCode)")
          }
        }
      } catch {
        case t: Throwable =>
          // Best-effort; never fail shutdown on cleanup.
          scribe.warn(s"OpenSearch shutdown cleanup failed: ${t.getMessage}")
      }
    }
  }
}