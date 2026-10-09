package spec

import lightdb.lucene.LuceneDurability

@EmbeddedTest
class SplitGroupCommitImmediateSpec extends AbstractSplitGroupCommitSpec {
  override protected def durability: LuceneDurability = LuceneDurability.Immediate
}
