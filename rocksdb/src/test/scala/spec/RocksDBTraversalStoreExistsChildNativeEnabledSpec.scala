package spec

import lightdb.store.CollectionManager
import org.scalatest.BeforeAndAfterAll
import profig.Profig

/**
 * Runs the core ExistsChild abstract spec with traversal native ExistsChild enabled.
 *
 * TraversalQueryEngine will either early-terminate semi-join for page-only queries, or fall back to planner resolution.
 */
@EmbeddedTest
class RocksDBTraversalStoreExistsChildNativeEnabledSpec
    extends AbstractExistsChildSpec
    with TraversalRocksDBWrappedManager
    with BeforeAndAfterAll {

  override def storeManager: CollectionManager = traversalStoreManager

  override protected def beforeAll(): Unit = {
    System.setProperty("lightdb.traversal.existsChild.native", "true")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally System.clearProperty("lightdb.traversal.existsChild.native")
  }
}


