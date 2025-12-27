package spec

import org.scalatest.BeforeAndAfterAll
import profig.Profig

/**
 * Centralized Profig bootstrap for test suites.
 *
 * Per project convention:
 * - Only tests should call `Profig.init()`.
 * - Tests should call `Profig.initConfiguration()` so file/env config is available.
 */
trait ProfigTestSupport extends BeforeAndAfterAll { this: org.scalatest.Suite =>
  override protected def beforeAll(): Unit = {
    if (!Profig.isLoaded) {
      Profig.init()
      Profig.initConfiguration()
    }
    super.beforeAll()
  }
}


