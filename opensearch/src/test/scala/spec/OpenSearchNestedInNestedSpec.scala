package spec

import fabric.rw.*
import lightdb.opensearch.OpenSearchStore
import profig.Profig

@EmbeddedTest
class OpenSearchNestedInNestedSpec extends AbstractNestedInNestedSpec with OpenSearchTestSupport {
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Profig("lightdb.opensearch.nested.fallback.allowed").store("true")
  }

  override protected def declareOuterNestedPath: Boolean = false
  override def storeManager: OpenSearchStore.type = OpenSearchStore
}
