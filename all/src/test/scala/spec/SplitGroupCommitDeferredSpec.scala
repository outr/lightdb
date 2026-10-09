package spec

import lightdb.lucene.LuceneDurability

import scala.concurrent.duration.DurationInt

@EmbeddedTest
class SplitGroupCommitDeferredSpec extends AbstractSplitGroupCommitSpec {
  override protected def durability: LuceneDurability = LuceneDurability.Deferred(250.millis)
}
