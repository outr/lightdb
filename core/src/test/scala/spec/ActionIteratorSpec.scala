package spec

import lightdb.util.ActionIterator
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * An ActionIterator closes once, at the end of its underlying iterator, and raises the underlying iterator's failure
 * after closing rather than ending as though it had run out.
 */
@EmbeddedTest
class ActionIteratorSpec extends AnyWordSpec with Matchers {
  "ActionIterator" should {
    "close once at the end" in {
      var closes = 0
      val it = ActionIterator(Iterator(1, 2, 3), onClose = () => closes += 1)
      it.toList shouldBe List(1, 2, 3)
      it.hasNext shouldBe false
      closes shouldBe 1
    }
    "raise a failure of the underlying iterator after closing" in {
      var closes = 0
      val failing = new Iterator[Int] {
        private var n = 0
        override def hasNext: Boolean = if n < 2 then true else throw new IllegalStateException("result set closed")
        override def next(): Int = { n += 1; n }
      }
      val it = ActionIterator(failing, onClose = () => closes += 1)
      it.next() shouldBe 1
      it.next() shouldBe 2
      an[IllegalStateException] should be thrownBy it.hasNext
      closes shouldBe 1
      it.hasNext shouldBe false
    }
  }
}
