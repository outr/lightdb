package lightdb.distance

import perfolation.double2Implicits

case class Distance(value: Double, unit: DistanceUnit) {
  def to(unit: DistanceUnit): Distance = {
    val meters = value * this.unit.asMeters
    val v = meters / unit.asMeters
    Distance(v, unit)
  }
  def toKilometers: Distance = to(DistanceUnit.Kilometers)
  def toMeters: Distance = to(DistanceUnit.Meters)
  def toCentimeters: Distance = to(DistanceUnit.Centimeters)
  def toMillimeters: Distance = to(DistanceUnit.Millimeters)
  def toMicrometers: Distance = to(DistanceUnit.Micrometers)
  def toNanometers: Distance = to(DistanceUnit.Nanometers)
  def toMiles: Distance = to(DistanceUnit.Miles)
  def toYards: Distance = to(DistanceUnit.Yards)
  def toFeet: Distance = to(DistanceUnit.Feet)
  def toInches: Distance = to(DistanceUnit.Inches)
  def toNauticalMiles: Distance = to(DistanceUnit.NauticalMiles)
  override def toString: String = s"${value.f(f = 4)} ${unit.abbreviation}"
}