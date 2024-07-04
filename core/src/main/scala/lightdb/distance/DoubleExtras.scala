package lightdb.distance

case class DoubleExtras(d: Double) extends AnyVal {
  def to(unit: DistanceUnit): Distance = Distance(d * unit.asMeters)

  def km: Distance = to(DistanceUnit.Kilometers)
  def m: Distance = to(DistanceUnit.Meters)
  def cm: Distance = to(DistanceUnit.Centimeters)
  def mm: Distance = to(DistanceUnit.Millimeters)
  def μm: Distance = to(DistanceUnit.Micrometers)
  def nm: Distance = to(DistanceUnit.Nanometers)
  def mi: Distance = to(DistanceUnit.Miles)
  def yd: Distance = to(DistanceUnit.Yards)
  def ft: Distance = to(DistanceUnit.Feet)
  def in: Distance = to(DistanceUnit.Inches)
  def NM: Distance = to(DistanceUnit.NauticalMiles)

  def kilometers: Distance = to(DistanceUnit.Kilometers)
  def meters: Distance = to(DistanceUnit.Meters)
  def centimeters: Distance = to(DistanceUnit.Centimeters)
  def millimeters: Distance = to(DistanceUnit.Millimeters)
  def micrometers: Distance = to(DistanceUnit.Micrometers)
  def nanometers: Distance = to(DistanceUnit.Nanometers)
  def miles: Distance = to(DistanceUnit.Miles)
  def yards: Distance = to(DistanceUnit.Yards)
  def feet: Distance = to(DistanceUnit.Feet)
  def inches: Distance = to(DistanceUnit.Inches)
  def nauticalMiles: Distance = to(DistanceUnit.NauticalMiles)
}
