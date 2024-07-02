package lightdb.distance

sealed trait DistanceUnit {
  lazy val name: String = getClass.getSimpleName.replace("$", "")

  def abbreviation: String

  def asMeters: Double
}

object DistanceUnit {
  case object Kilometers extends DistanceUnit {
    override def abbreviation: String = "km"
    override def asMeters: Double = 1000.0
  }
  case object Meters extends DistanceUnit {
    override def abbreviation: String = "m"
    override def asMeters: Double = 1.0
  }
  case object Centimeters extends DistanceUnit {
    override def abbreviation: String = "cm"
    override def asMeters: Double = 0.01
  }
  case object Millimeters extends DistanceUnit {
    override def abbreviation: String = "mm"
    override def asMeters: Double = 0.001
  }
  case object Micrometers extends DistanceUnit {
    override def abbreviation: String = "μm"
    override def asMeters: Double = 1e+6
  }
  case object Nanometers extends DistanceUnit {
    override def abbreviation: String = "nm"
    override def asMeters: Double = 1e-9
  }
  case object Miles extends DistanceUnit {
    override def abbreviation: String = "mi"
    override def asMeters: Double = 1609.34
  }
  case object Yards extends DistanceUnit {
    override def abbreviation: String = "yd"
    override def asMeters: Double = 0.9144
  }
  case object Feet extends DistanceUnit {
    override def abbreviation: String = "ft"
    override def asMeters: Double = 0.3048
  }
  case object Inches extends DistanceUnit {
    override def abbreviation: String = "in"
    override def asMeters: Double = 0.0254
  }
  case object NauticalMiles extends DistanceUnit {
    override def abbreviation: String = "nm"
    override def asMeters: Double = 1852.0
  }
}