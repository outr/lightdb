package spec

import lightdb.lucene.LuceneDurability

@EmbeddedTest
class SplitRollbackImmediateSpec extends AbstractSplitRollbackSpec {
  override protected def durability: LuceneDurability = LuceneDurability.Immediate
}
