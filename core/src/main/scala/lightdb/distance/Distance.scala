package lightdb.distance

import perfolation.double2Implicits

case class Distance(valueInMeters: Double) extends AnyVal {
  def to(unit: DistanceUnit): Double = valueInMeters / unit.asMeters
  def toKilometers: Double = to(DistanceUnit.Kilometers)
  def toMeters: Double = to(DistanceUnit.Meters)
  def toCentimeters: Double = to(DistanceUnit.Centimeters)
  def toMillimeters: Double = to(DistanceUnit.Millimeters)
  def toMicrometers: Double = to(DistanceUnit.Micrometers)
  def toNanometers: Double = to(DistanceUnit.Nanometers)
  def toMiles: Double = to(DistanceUnit.Miles)
  def toYards: Double = to(DistanceUnit.Yards)
  def toFeet: Double = to(DistanceUnit.Feet)
  def toInches: Double = to(DistanceUnit.Inches)
  def toNauticalMiles: Double = to(DistanceUnit.NauticalMiles)

  def km: Double = toKilometers
  def m: Double = toMeters
  def cm: Double = toCentimeters
  def mm: Double = toMillimeters
  def μm: Double = toMicrometers
  def nm: Double = toNanometers
  def mi: Double = toMiles
  def yd: Double = toYards
  def ft: Double = toFeet
  def in: Double = toInches
  def NM: Double = toNauticalMiles

  def format(unit: DistanceUnit, minimumFractionDigits: Int = 2): String =
    s"${to(unit).f(f = minimumFractionDigits)} ${unit.abbreviation}"

  override def toString: String = format(DistanceUnit.Meters)
}