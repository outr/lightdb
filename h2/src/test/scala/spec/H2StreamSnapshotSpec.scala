package spec

import lightdb.h2.H2Store

@EmbeddedTest
class H2StreamSnapshotSpec extends AbstractStreamSnapshotSpec {
  override def storeManager: H2Store.type = H2Store
}
