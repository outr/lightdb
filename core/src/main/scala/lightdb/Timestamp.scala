package lightdb

import fabric.define.DefType
import fabric.rw._
import perfolation.long2Implicits

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

  def toMidnight: Timestamp = Timestamp.of(
    year = year,
    month = month,
    day = day
  )

  def isExpired(timeout: FiniteDuration): Boolean = {
    val now = System.currentTimeMillis()
    value + timeout.toMillis < now
  }

  override def toString: String = s"${value.t.D} ${value.t.T}"
}

object Timestamp {
  implicit val rw: RW[Timestamp] = RW.from(
    r = _.value.json,
    w = j => Timestamp(j.asLong),
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