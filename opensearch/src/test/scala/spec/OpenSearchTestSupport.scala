package spec

/**
 * Test support for OpenSearch-backed specs.
 *
 * Defaults:
 * - Prefer Testcontainers for ad-hoc OpenSearch startup (set `-Dlightdb.opensearch.useTestcontainers=false` to disable)
 * - Fallback to `http://localhost:9200` if Testcontainers is disabled or Docker is unavailable
 * - Use refreshPolicy `true` for deterministic visibility in tests
 */
trait OpenSearchTestSupport extends ProfigTestSupport { this: org.scalatest.Suite =>
  import fabric.rw._
  import profig.Profig

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initOpenSearchTestConfig()
  }

  private def probeOpenSearch(baseUrl: String): Boolean = {
    // Lightweight probe to detect an already-running local OpenSearch node.
    // We avoid instantiating the OpenSearch client here to keep bootstrap simple.
    try {
      val url = new java.net.URL(baseUrl + "/")
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
      Profig("lightdb.opensearch.indexPrefix").store(s"lightdb_test_${java.util.UUID.randomUUID().toString.replace("-", "")}")
    }
  }
}