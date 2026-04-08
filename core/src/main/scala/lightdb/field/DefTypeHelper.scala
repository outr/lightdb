package lightdb.field

import fabric.define.DefType

object DefTypeHelper {
  /**
   * Deeply unwraps `Classed` and `Described` wrappers from a DefType tree, returning the underlying
   * structural type with all nested wrappers removed. This ensures pattern matches against
   * DefType.Str, DefType.Int, DefType.Opt(DefType.Str, _), etc. work correctly regardless of
   * whether any level is wrapped with class/description metadata.
   */
  def unwrap(d: DefType): DefType = d match {
    case DefType.Classed(dt, _) => unwrap(dt)
    case DefType.Described(dt, _) => unwrap(dt)
    case DefType.Opt(dt, desc) => DefType.Opt(unwrap(dt), desc)
    case DefType.Arr(dt, desc) => DefType.Arr(unwrap(dt), desc)
    case DefType.Obj(map, cn, desc) => DefType.Obj(map.map { case (k, v) => k -> unwrap(v) }, cn, desc)
    case DefType.Poly(map, cn, desc) => DefType.Poly(map.map { case (k, v) => k -> unwrap(v) }, cn, desc)
    case _ => d
  }
}
