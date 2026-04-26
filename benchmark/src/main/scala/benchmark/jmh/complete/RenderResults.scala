package benchmark.jmh.complete

import fabric.*
import fabric.io.JsonParser

import java.nio.file.{Files, Path, Paths}

/** Turn JMH JSON output into a human-readable Markdown report + one SVG bar chart per
 *  benchmark.
 *
 *  Usage (from sbt):
 *  {{{
 *    benchmark/runMain benchmark.jmh.complete.RenderResults \
 *      --out benchmark/target/report-fast-20260426 \
 *      --title "LightDB Backends — fast" \
 *      benchmark/target/jmh-results-fast-*.json
 *  }}}
 *
 *  Output layout (under `--out`):
 *  - `report.md`               — single Markdown file with one section per benchmark, table +
 *                                inline SVG `<img>` link
 *  - `<benchmark>.svg`         — one horizontal bar chart per benchmark
 *
 *  The renderer is intentionally dependency-free apart from `fabric` (which is already on
 *  the LightDB classpath). SVG is hand-emitted as plain XML — no charting library, no JS.
 */
object RenderResults {
  def main(args: Array[String]): Unit = {
    val parsed = parseArgs(args.toList)
    val outDir = parsed.outDir
    Files.createDirectories(outDir)

    val results: List[Result] = parsed.inputs.flatMap(loadJmh)
    if results.isEmpty then {
      System.err.println("[render] no benchmark results found in inputs; exiting")
      sys.exit(1)
    }

    // Group by benchmark name. Within each group: one entry per (backend, secondary-params)
    // tuple. We use the full param map as the bar label so multi-param benchmarks stay distinct.
    val byBench: Map[String, List[Result]] =
      results.groupBy(_.benchmark).view.mapValues(_.sortBy(-_.score)).toMap

    val mdSb = new StringBuilder
    mdSb ++= s"# ${parsed.title}\n\n"
    mdSb ++= s"_Generated from ${parsed.inputs.size} JMH JSON file(s) covering "
    mdSb ++= s"${results.size} measurements across ${byBench.size} benchmarks._\n\n"
    mdSb ++= "**Higher is better** for `thrpt` (ops/s); **lower is better** for `avgt`/`ss` (ms).\n\n"

    // Stable benchmark ordering — by class then method.
    byBench.keys.toList.sorted.foreach { bench =>
      val rows = byBench(bench)
      val unit = rows.head.unit
      mdSb ++= s"## $bench\n\n"
      mdSb ++= s"_${rows.size} runs · unit: ${unit}_\n\n"
      mdSb ++= renderTable(rows)
      mdSb ++= "\n"

      val svgFile = outDir.resolve(safeName(bench) + ".svg")
      val svg = renderSvg(bench, rows)
      Files.writeString(svgFile, svg)
      mdSb ++= s"![${bench}](./${svgFile.getFileName})\n\n"
    }

    val mdPath = outDir.resolve("report.md")
    Files.writeString(mdPath, mdSb.toString)
    println(s"[render] wrote $mdPath")
    println(s"[render] wrote ${byBench.size} SVG chart(s) to $outDir")
  }

  // ---- argument parsing -------------------------------------------------------------------

  private final case class Args(outDir: Path, title: String, inputs: List[Path])

  private def parseArgs(args: List[String]): Args = {
    var out: Option[Path] = None
    var title: String = "LightDB Benchmarks"
    val inputs = scala.collection.mutable.ListBuffer.empty[Path]
    var i = 0
    val arr = args.toArray
    while i < arr.length do {
      arr(i) match {
        case "--out" =>
          out = Some(Paths.get(arr(i + 1))); i += 2
        case "--title" =>
          title = arr(i + 1); i += 2
        case other if other.startsWith("--") =>
          throw new IllegalArgumentException(s"unknown flag: $other")
        case other =>
          inputs += Paths.get(other); i += 1
      }
    }
    if inputs.isEmpty then throw new IllegalArgumentException("at least one JSON input is required")
    Args(out.getOrElse(Paths.get(".")), title, inputs.toList)
  }

  // ---- JMH JSON parsing -------------------------------------------------------------------

  /** One JMH measurement: a benchmark + its param tuple + the primary score. */
  private final case class Result(
    benchmark: String,
    mode: String,
    unit: String,
    params: Map[String, String],
    score: Double,
    error: Double
  ) {
    /** Compact label for chart bars: `"<backend> [other=...]"`. */
    def label: String = {
      val backend = params.getOrElse("backend", "?")
      val rest = params.removed("backend").removed("recordCount").toList.sortBy(_._1)
      if rest.isEmpty then backend
      else backend + rest.map { case (k, v) => s" $k=$v" }.mkString(" [", "", "]")
    }
  }

  private def loadJmh(path: Path): List[Result] = {
    val text = Files.readString(path)
    val json = JsonParser(text)
    json match {
      case Arr(values, _) => values.toList.flatMap(parseEntry)
      case _ =>
        System.err.println(s"[render] $path: top-level JSON is not an array; skipping")
        Nil
    }
  }

  private def parseEntry(json: Json): Option[Result] = {
    json match {
      case obj: Obj =>
        val bench = obj.value.get("benchmark").map(_.asString).getOrElse("<unknown>")
        val mode = obj.value.get("mode").map(_.asString).getOrElse("?")
        val params: Map[String, String] = obj.value.get("params") match {
          case Some(p: Obj) => p.value.map { case (k, v) =>
            k -> (v match {
              case Str(s, _) => s
              case other => other.toString
            })
          }.toMap
          case _ => Map.empty
        }
        val pm = obj.value.get("primaryMetric")
        pm match {
          case Some(metric: Obj) =>
            val score = metric.value.get("score").map(_.asDouble).getOrElse(Double.NaN)
            val error = metric.value.get("scoreError")
              .flatMap {
                case n: NumDec => Some(n.value.toDouble)
                case n: NumInt => Some(n.value.toDouble)
                case Str(s, _) => s.toDoubleOption
                case _ => None
              }
              .getOrElse(0.0)
            val unit = metric.value.get("scoreUnit").map(_.asString).getOrElse("")
            Some(Result(bench, mode, unit, params, score, error))
          case _ => None
        }
      case _ => None
    }
  }

  // ---- markdown ---------------------------------------------------------------------------

  private def renderTable(rows: List[Result]): String = {
    val sb = new StringBuilder
    sb ++= "| Backend | Score | ± Error | Unit |\n"
    sb ++= "|---|---:|---:|---|\n"
    rows.foreach { r =>
      sb ++= s"| ${r.label} | ${fmt(r.score)} | ${fmt(r.error)} | ${r.unit} |\n"
    }
    sb.toString
  }

  // ---- SVG bar chart ----------------------------------------------------------------------

  /** Horizontal bar chart. One bar per result, sorted by score descending. Width is
   *  proportional to score (or 1/score for latency-style benchmarks where smaller is better,
   *  detected by mode `avgt`/`ss`).
   */
  private def renderSvg(benchmark: String, rows: List[Result]): String = {
    val padLeft = 240
    val padRight = 120
    val padTop = 40
    val padBottom = 30
    val barH = 22
    val barGap = 8

    val isLatency = rows.headOption.exists(r => r.mode == "avgt" || r.mode == "ss" || r.mode == "sample")
    // For latency-mode, lower is better — sort ascending and invert width scaling.
    val sorted = if isLatency then rows.sortBy(_.score) else rows.sortBy(-_.score)
    val maxScore = sorted.map(_.score).maxOption.getOrElse(1.0).max(1e-9)
    val width = padLeft + 480 + padRight
    val height = padTop + sorted.size * (barH + barGap) + padBottom

    val sb = new StringBuilder
    sb ++= s"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 $width $height" font-family="-apple-system, Segoe UI, Helvetica, Arial, sans-serif" font-size="12">\n"""
    sb ++= s"""  <rect width="$width" height="$height" fill="#ffffff"/>\n"""
    sb ++= s"""  <text x="${width / 2}" y="22" text-anchor="middle" font-size="14" font-weight="600">${escape(benchmark)}</text>\n"""
    val unit = rows.headOption.map(_.unit).getOrElse("")
    val direction = if isLatency then "lower is better" else "higher is better"
    sb ++= s"""  <text x="${width / 2}" y="36" text-anchor="middle" fill="#666">unit: $unit · $direction</text>\n"""

    sorted.zipWithIndex.foreach { case (r, idx) =>
      val y = padTop + idx * (barH + barGap)
      val barW = ((r.score / maxScore) * 480).max(1).toInt
      val color = pickColor(r.label)
      // Label
      sb ++= s"""  <text x="${padLeft - 8}" y="${y + barH * 2 / 3}" text-anchor="end" fill="#222">${escape(r.label)}</text>\n"""
      // Bar
      sb ++= s"""  <rect x="$padLeft" y="$y" width="$barW" height="$barH" rx="3" fill="$color"/>\n"""
      // Score on the right
      sb ++= s"""  <text x="${padLeft + barW + 6}" y="${y + barH * 2 / 3}" fill="#222">${fmt(r.score)}</text>\n"""
    }
    sb ++= "</svg>\n"
    sb.toString
  }

  /** Stable color per backend: hash → HSL, kept saturated/dark enough to read on white. */
  private def pickColor(label: String): String = {
    // Backend name only (strip secondary params for consistent color across charts).
    val backend = label.takeWhile(_ != ' ')
    val h = (math.abs(backend.hashCode) % 360)
    s"hsl($h, 65%, 45%)"
  }

  // ---- helpers -----------------------------------------------------------------------------

  private def fmt(d: Double): String = {
    if d.isNaN then "—"
    else if math.abs(d) >= 1000 then f"$d%,.0f"
    else if math.abs(d) >= 10   then f"$d%,.1f"
    else                              f"$d%.3f"
  }

  private def safeName(bench: String): String =
    bench.replace('.', '_').replace('/', '_')

  private def escape(s: String): String =
    s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
}
