package lightdb.time

import java.time.{DateTimeException, Instant, LocalDate, LocalDateTime, OffsetDateTime, ZoneId, ZonedDateTime}
import java.time.format.{DateTimeFormatter, ResolverStyle}
import java.time.temporal.TemporalAccessor
import scala.util.Try

object TimestampParser {
  /**
   * Try to detect the date/time in `s` and return Timestamp.
   *
   * @param s              input text (date/time or raw epoch)
   * @param defaultZone    used when the input has no zone/offset (e.g., "2025-09-18 07:30")
   * @param preferDayFirst if true, try dd/MM/yyyy patterns before MM/dd/yyyy
   */
  def apply(s: String,
            defaultZone: ZoneId = ZoneId.systemDefault(),
            preferDayFirst: Boolean = false): Option[Timestamp] = {
    val in = s.trim

    // 1) Pure digits? Treat as epoch seconds or millis.
    if (in.matches("^-?\\d+$")) {
      val n = Try(in.toLong).toOption
      n.map { v =>
        // Heuristic: 13+ digits => millis; 10 digits => seconds.
        if (math.abs(v) >= 100000000000L) v else v * 1000L
      }.map(Timestamp.apply)
    } else {
      // 2) Try very common, highly standard formats first (fast path).
      val fast = List(
        Try(Instant.parse(in)).map(_.toEpochMilli),
        Try(ZonedDateTime.parse(in, DateTimeFormatter.ISO_ZONED_DATE_TIME)).map(_.toInstant.toEpochMilli),
        Try(OffsetDateTime.parse(in, DateTimeFormatter.ISO_OFFSET_DATE_TIME)).map(_.toInstant.toEpochMilli),
        Try(OffsetDateTime.parse(in, DateTimeFormatter.RFC_1123_DATE_TIME)).map(_.toInstant.toEpochMilli)
      ).collectFirst { case t if t.isSuccess => t.get }
      if (fast.isDefined) {
        fast.map(Timestamp.apply)
      } else {
        // 3) Broader set of patterns (both US/EU styles, with/without time/zone).
        val patterns = buildPatterns(preferDayFirst)

        val attempt = patterns.view.flatMap { fmt =>
          // Use parseBest so we can handle Zoned/Offset/Local/Date with one formatter.
          val parsed: Option[TemporalAccessor] =
            Try(fmt.parse(in)).toOption

          parsed.flatMap { ta =>
            // If the parsed object already contains an instant, use it
            val millisFromInstant =
              Try(Instant.from(ta).toEpochMilli).toOption
            millisFromInstant.orElse {
              // Otherwise, determine if we have date-time or just a date
              val best =
                Try(fmt.parseBest(in,
                  ZonedDateTime.from _,
                  OffsetDateTime.from _,
                  LocalDateTime.from _,
                  LocalDate.from _
                )).toOption

              best.map {
                case z: ZonedDateTime => z.toInstant.toEpochMilli
                case o: OffsetDateTime => o.toInstant.toEpochMilli
                case ldt: LocalDateTime => ldt.atZone(defaultZone).toInstant.toEpochMilli
                case d: LocalDate => d.atStartOfDay(defaultZone).toInstant.toEpochMilli
                case _ => throw new DateTimeException("Unsupported temporal type")
              }
            }
          }
        }.headOption
        attempt.map(Timestamp.apply)
      }
    }
  }

  private def buildPatterns(preferDayFirst: Boolean): List[DateTimeFormatter] = {
    val locale = java.util.Locale.US

    // Core date skeletons
    val ymd = "uuuu-MM-dd"
    val basic = "uuuuMMdd"
    val mdy = "MM/dd/uuuu"
    val dmy = "dd/MM/uuuu"
    val dMonY = "dd-MMM-uuuu"

    // 24h times
    val t24 = List("HH:mm", "HH:mm:ss", "HH:mm:ss.SSS")
    // 12h times
    val t12 = List("h:mm a", "h:mm:ss a")

    // joiners between date and time
    val joins = List(" ", "'T'") // space or literal T

    // zone suffixes
    val zones = List("", " XXX", " VV") // ISO offset "+02:00" / zone id "Europe/Paris"

    def fmt(p: String) =
      DateTimeFormatter.ofPattern(p).withResolverStyle(ResolverStyle.SMART).withLocale(locale)

    // Generate [date] + optional ([join] + [time]) + optional [zone]
    def cross(date: String, times: List[String]) = {
      val base = List(date)
      val with24 = for (j <- joins; t <- t24) yield s"$date$j$t"
      val with12 = for (j <- joins; t <- t12) yield s"$date$j$t"
      // Attach zone variants
      (base ++ with24 ++ with12).flatMap(p => zones.map(z => (p + (if (z.isEmpty) "" else s" $z")).trim))
    }

    // Order matters: choose ambiguous preference
    val dateOrder =
      if (preferDayFirst) List(dmy, mdy) else List(mdy, dmy)

    val allPatterns: List[String] =
      List(ymd, basic, dMonY) ++ dateOrder ++
        cross(ymd, t24 ++ t12) ++
        cross(basic, t24 ++ t12) ++
        cross(dMonY, t24 ++ t12) ++
        dateOrder.flatMap(d => cross(d, t24 ++ t12))

    // Always include a couple of extras that aren’t covered above:
    val extras = List(
      // RFC1123 already tried in fast path, but include here in case
      DateTimeFormatter.RFC_1123_DATE_TIME,
      // ISO local date-time with a space, common in logs (no zone)
      DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm[:ss][.SSS]").withResolverStyle(ResolverStyle.SMART).withLocale(locale)
    )

    allPatterns.distinct.map(fmt) ::: extras
  }
}
