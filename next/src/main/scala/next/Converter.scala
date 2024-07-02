package next

trait Converter[From, To] {
  def convert(from: From): To
}
