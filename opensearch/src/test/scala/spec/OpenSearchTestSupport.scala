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
    val resolvedBaseUrl: String = if (useTestcontainers && currentBaseUrl == defaultLocal) {
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