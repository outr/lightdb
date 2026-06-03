package spec

import lightdb.time.{Timestamp, TimestampParser}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import fabric.*
import fabric.rw.*

import java.time.Instant

@EmbeddedTest
class TimestampSpec extends AnyWordSpec with Matchers {
  // The expected value is the absolute instant the RFC 3339 string denotes. Comparing
  // against the epoch millis (rather than a `Timestamp.of(...)` built from local fields)
  // keeps these assertions independent of the JVM's default time zone, which differs
  // between dev machines (US Central) and CI (UTC).
  private val rfc3339 = "2025-07-22T14:13:33.371Z"
  private val expected = Timestamp(Instant.parse(rfc3339).toEpochMilli)

  "Timestamp" should {
    "parse a string timestamp RFC 3339" in {
      TimestampParser(rfc3339) should be(Some(expected))
    }
    "parse a string timestamp RFC 3339 from JSON" in {
      str(rfc3339).as[Timestamp] should be(expected)
    }
  }
}
