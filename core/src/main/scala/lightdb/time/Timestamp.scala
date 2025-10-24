package lightdb.time

import fabric.{NumInt, Str}
import fabric.define.DefType
import fabric.rw._
import perfolation.long2Implicits
import lightdb._

import java.util.Calendar
import scala.concurrent.duration.FiniteDuration

case class Timestamp(value: Long = System.currentTimeMillis()) extends AnyVal {
  def year: Int = value.t.year
  def month: Int = value.t.month + 1
  def day: Int = value.t.dayOfMonth
  def hour: Int = value.t.hour24
  def minute: Int = value.t.minuteOfHour
  def second: Int = value.t.secondOfMinute
  def millisecond: Int = value.t.milliOfSecond

  def daysBetween(that: Timestamp): Int = {
    val millisPerDay = 86_400_000L
    val start = math.min(value, that.value)
    val end = math.max(value, that.value)
    math.floorDiv(end - start, millisPerDay).toInt
  }

  def toMidnight: Timestamp = Timestamp.of(
    year = year,
    month = month,
    day = day
  )

  def isExpired(timeout: FiniteDuration): Boolean = {
    val now = System.currentTimeMillis()
    value + timeout.toMillis < now
  }

  override def toString: String = s"${value.t.m}/${value.t.d}/${value.t.year} ${value.t.T}"
}

object Timestamp {
  /**
   * Convenience method to get the current year.
   */
  def CurrentYear: Int = Timestamp().year

  implicit val rw: RW[Timestamp] = RW.from(
    r = _.value.json,
    w = {
      case NumInt(l, _) => Timestamp(l)
      case Str(s, _) => TimestampParser(s).getOrElse(throw new RuntimeException(s"Unable to parse Timestamp from: [$s]"))
      case j => throw new RuntimeException(s"Unsupported JSON for Timestamp: $j")
    },
    d = DefType.Int
  )
  implicit val numeric: Numeric[Timestamp] = Numeric[Long].map(Timestamp.apply)(_.value)

  def fromCalendar(calendar: Calendar): Timestamp = Timestamp(calendar.getTimeInMillis)

  def of(year: Int = now.year,
         month: Int = now.month,
         day: Int = now.day,
         hour: Int = 0,
         minute: Int = 0,
         second: Int = 0,
         millisecond: Int = 0): Timestamp = {
    val c = Calendar.getInstance()
    c.set(Calendar.MILLISECOND, millisecond)
    c.set(year, month - 1, day, hour, minute, second)
    fromCalendar(c)
  }

  def month(s: String): Int = s.toLowerCase match {
    case "january" | "jan" => 1
    case "february" | "feb" => 2
    case "march" | "mar" => 3
    case "april" | "apr" => 4
    case "may" => 5
    case "june" | "jun" => 6
    case "july" | "jul" => 7
    case "august" | "aug" => 8
    case "september" | "sep" | "sept" => 9
    case "october" | "oct" => 10
    case "november" | "nov" => 11
    case "december" | "dec" => 12
  }

  def now: Timestamp = Timestamp()
}