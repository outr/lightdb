package spec

import lightdb.time.{Timestamp, TimestampParser}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import fabric.*
import fabric.rw.*

import java.util.{Locale, TimeZone}

@EmbeddedTest
class TimestampSpec extends AnyWordSpec with Matchers {
  "Timestamp" should {
    "init" in {
      TimeZone.setDefault(TimeZone.getTimeZone("America/Chicago"))
      Locale.setDefault(Locale.US)
    }
    "parse a string timestamp RFC 3339" in {
      TimestampParser("2025-07-22T14:13:33.371Z") should be(Some(Timestamp.of(
        year = 2025,
        month = 7,
        day = 22,
        hour = 9,
        minute = 13,
        second = 33,
        millisecond = 371
      )))
    }
    "parse a string timestamp RFC 3339 from JSON" in {
      str("2025-07-22T14:13:33.371Z").as[Timestamp] should be(Timestamp.of(
        year = 2025,
        month = 7,
        day = 22,
        hour = 9,
        minute = 13,
        second = 33,
        millisecond = 371
      ))
    }
  }
}
