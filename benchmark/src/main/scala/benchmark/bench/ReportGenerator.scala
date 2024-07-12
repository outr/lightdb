package benchmark.bench

import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._
import io.quickchart.QuickChart
import org.apache.commons.io.FileUtils

import java.io.File

object ReportGenerator {
  private lazy val quickChart = {
    val qc = new QuickChart
    qc.setWidth(1920)
    qc.setHeight(1080)
    qc
  }

  private lazy val outputDirectory = new File("charts")

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(outputDirectory)
    outputDirectory.mkdirs()

    val directory = new File(".")
    val files = directory.listFiles().toList.filter { file =>
      val name = file.getName
      name.startsWith("report-") && name.endsWith(".json")
    }
    val reportsMap = files.flatMap { file =>
      val name = file.getName.substring(7, file.getName.length - 5)
      JsonParser(file).as[List[BenchmarkReport]].map(r => (name, r))
    }.groupBy(_._2.name)
    reportsMap.foreach {
      case (name, nameAndReports) =>
        totalTimeReport(name, nameAndReports)
    }
  }

  private def totalTimeReport(name: String, nameAndReports: List[(String, BenchmarkReport)]): Unit = {
    val names = nameAndReports.map(_._1)
    val reports = nameAndReports.map(_._2)
    val chart = Chart(
      `type` = "bar",
      data = ChartData(
        labels = names,
        datasets = List(
          ChartDataSet(
            label = reports.head.name,
            data = reports.map(_.logs.last.elapsed),
            backgroundColor = "blue"
          )
        )
      ),
      options = Some(ChartOptions(
        title = Some(ChartTitle(
          text = s"$name: Total Time",
          fontSize = 48,
          fontColor = "white"
        )),
        legend = Some(ChartLegend(
          labels = ChartLabel(
            fontSize = Some(24),
            fontColor = Some("white")
          )
        )),
        scales = Some(ChartScales(
          xAxes = List(
            ChartScale(
              scaleLabel = ChartLabel(fontSize = Some(32), labelString = Some("Implementation")),
              ticks = ChartTicks(fontSize = Some(24), fontColor = Some("white"))
            )
          ),
          yAxes = List(
            ChartScale(
              scaleLabel = ChartLabel(fontSize = Some(32), labelString = Some("Seconds Elapsed")),
              ticks = ChartTicks(fontSize = Some(24))
            )
          )
        ))
      ))
    )
    quickChart.setConfig(chart.config)
    quickChart.toFile(s"${outputDirectory.getName}/$name-total-time.png")
  }
}

case class Chart(`type`: String, data: ChartData, options: Option[ChartOptions] = None) {
  lazy val config: String = {
    val jsonString = JsonFormatter.Compact(this.json)
    jsonString
      .replace("\"red\"", "getGradientFillHelper('vertical', [\"#c40000\", \"#420104\"])")
      .replace("\"green\"", "getGradientFillHelper('vertical', [\"#27c400\", \"#034201\"])")
      .replace("\"blue\"", "getGradientFillHelper('vertical', [\"#0a00c4\", \"#050142\"])")
      .replace("\"yellow\"", "getGradientFillHelper('vertical', [\"#c4c400\", \"#424201\"])")
      .replace("\"cyan\"", "getGradientFillHelper('vertical', [\"#00c4bb\", \"#013a42\"])")
      .replace("\"purple\"", "getGradientFillHelper('vertical', [\"#d1020c\", \"#3e0142\"])")
      .replace("\"orange\"", "getGradientFillHelper('vertical', [\"#c44500\", \"#421801\"])")
  }
}

object Chart {
  implicit val rw: RW[Chart] = RW.gen
}

case class ChartData(labels: List[String], datasets: List[ChartDataSet])

object ChartData {
  implicit val rw: RW[ChartData] = RW.gen
}

case class ChartDataSet(label: String, data: List[Double], backgroundColor: String)

object ChartDataSet {
  implicit val rw: RW[ChartDataSet] = RW.gen
}

case class ChartOptions(title: Option[ChartTitle] = None,
                        legend: Option[ChartLegend] = None,
                        scales: Option[ChartScales] = None)

object ChartOptions {
  implicit val rw: RW[ChartOptions] = RW.gen
}

case class ChartTitle(text: String,
                      fontSize: Int,
                      fontColor: String,
                      display: Boolean = true)

object ChartTitle {
  implicit val rw: RW[ChartTitle] = RW.gen
}

case class ChartLegend(labels: ChartLabel)

object ChartLegend {
  implicit val rw: RW[ChartLegend] = RW.gen
}

case class ChartScales(xAxes: List[ChartScale], yAxes: List[ChartScale])

object ChartScales {
  implicit val rw: RW[ChartScales] = RW.gen
}

case class ChartScale(scaleLabel: ChartLabel, ticks: ChartTicks)

object ChartScale {
  implicit val rw: RW[ChartScale] = RW.gen
}

case class ChartLabel(display: Boolean = true,
                      fontColor: Option[String] = None,
                      fontSize: Option[Int] = None,
                      fontStyle: Option[String] = None,
                      labelString: Option[String] = None)

object ChartLabel {
  implicit val rw: RW[ChartLabel] = RW.gen
}

case class ChartTicks(display: Boolean = true,
                      beginAtZero: Boolean = false,
                      fontColor: Option[String] = None,
                      fontSize: Option[Int] = None,
                      fontStyle: Option[String] = None,
                      labelString: Option[String] = None)

object ChartTicks {
  implicit val rw: RW[ChartTicks] = RW.gen
}