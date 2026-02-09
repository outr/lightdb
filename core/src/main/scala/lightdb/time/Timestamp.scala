package lightdb.time

import fabric.*
import fabric.define.DefType
import fabric.rw.*
import perfolation.long2Implicits
import lightdb.*

import java.util.Calendar
import scala.concurrent.duration.*

case class Timestamp(value: Long = System.currentTimeMillis()) extends AnyVal {
  def year: Int = apply(Timestamp.Field.Year)
  def month: Int = apply(Timestamp.Field.Month)
  def day: Int = apply(Timestamp.Field.Day)
  def hour: Int = apply(Timestamp.Field.Hour)
  def minute: Int = apply(Timestamp.Field.Minute)
  def second: Int = apply(Timestamp.Field.Second)
  def millisecond: Int = apply(Timestamp.Field.Millisecond)

  def apply(field: Timestamp.Field): Int = field match {
    case Timestamp.Field.Year => value.t.year
    case Timestamp.Field.Month => value.t.month + 1
    case Timestamp.Field.Day => value.t.dayOfMonth
    case Timestamp.Field.Hour => value.t.hour24
    case Timestamp.Field.Minute => value.t.minuteOfHour
    case Timestamp.Field.Second => value.t.secondOfMinute
    case Timestamp.Field.Millisecond => value.t.milliOfSecond
  }

  def plus(field: Timestamp.Field, amount: Int): Timestamp = if (amount == 0) {
    this
  } else {
    val c = Calendar.getInstance()
    c.setTimeInMillis(value)
    c.add(field.calendarField, amount)
    Timestamp(c.getTimeInMillis)
  }

  def minus(field: Timestamp.Field, amount: Int): Timestamp = plus(field, -amount)

  def durationFromNow: FiniteDuration = {
    val now = System.currentTimeMillis()
    if value > now then (value - now).millis else 0.millis
  }

  def +(millis: Long): Timestamp = Timestamp(value + millis)
  def -(millis: Long): Timestamp = Timestamp(value - millis)
  
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
  enum Field(val calendarField: Int) {
    case Year extends Field(java.util.Calendar.YEAR)
    case Month extends Field(java.util.Calendar.MONTH)
    case Day extends Field(java.util.Calendar.DAY_OF_MONTH)
    case Hour extends Field(java.util.Calendar.HOUR_OF_DAY)
    case Minute extends Field(java.util.Calendar.MINUTE)
    case Second extends Field(java.util.Calendar.SECOND)
    case Millisecond extends Field(java.util.Calendar.MILLISECOND)
  }

  /**
   * Convenience method to get the current year.
   */
  def CurrentYear: Int = Timestamp().year

  val MillisecondsJson: Timestamp => Json = _.value.json
  val YearMonthDayJson: Timestamp => Json = ts => str(s"${ts.value.t.Y}-${ts.value.t.m}-${ts.value.t.d}")

  var ToJson: Timestamp => Json = MillisecondsJson

  implicit val rw: RW[Timestamp] = RW.from(
    r = (ts: Timestamp) => ToJson(ts),
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