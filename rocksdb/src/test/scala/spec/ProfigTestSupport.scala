package spec

import org.scalatest.{BeforeAndAfterAll, Suite}
import profig.Profig

/**
 * Centralized Profig bootstrap for RocksDB traversal test suites.
 *
 * Convention:
 * - Only tests call `Profig.init()`
 * - Tests call `Profig.initConfiguration()` so file/env config is available
 */
trait ProfigTestSupport extends BeforeAndAfterAll { this: Suite =>
  override protected def beforeAll(): Unit = {
    if !Profig.isLoaded then {
      Profig.init()
      Profig.initConfiguration()
    }
    super.beforeAll()
  }
}


