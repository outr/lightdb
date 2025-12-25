package spec

/**
 * Test support for OpenSearch-backed specs.
 *
 * Defaults:
 * - Prefer Testcontainers for ad-hoc OpenSearch startup (set `-Dlightdb.opensearch.useTestcontainers=false` to disable)
 * - Fallback to `http://localhost:9200` if Testcontainers is disabled or Docker is unavailable
 * - Use refreshPolicy `true` for deterministic visibility in tests
 */
trait OpenSearchTestSupport {
  // Docker daemon on some hosts enforces a minimum API version. Testcontainers' shaded docker-java reads this from
  // the system property key `api.version`.
  sys.props.getOrElseUpdate("api.version", "1.44")

  private val useTestcontainers: Boolean =
    sys.props.get("lightdb.opensearch.useTestcontainers").forall(_.toBoolean)

  private val defaultLocal = "http://localhost:9200"
  private val currentBaseUrl = sys.props.getOrElse("lightdb.opensearch.baseUrl", defaultLocal)
  private val baseUrl: String = if (useTestcontainers && currentBaseUrl == defaultLocal) {
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

  sys.props.getOrElseUpdate("lightdb.opensearch.baseUrl", baseUrl)
  // Use `true` for deterministic visibility in tests. (Some OpenSearch APIs only accept true/false.)
  sys.props.getOrElseUpdate("lightdb.opensearch.refreshPolicy", "true")
  // Enable request logging in tests to aid debugging when running under Spice HttpClient.
  sys.props.getOrElseUpdate("lightdb.opensearch.logRequests", "true")
  // Use a per-JVM unique prefix to avoid index/mapping reuse across reruns (and between suites).
  sys.props.getOrElseUpdate("lightdb.opensearch.indexPrefix", s"lightdb_test_${java.util.UUID.randomUUID().toString.replace("-", "")}")
}


