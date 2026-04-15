package lightdb.field

import fabric.define.{DefType, Definition}

/**
 * Bridges the fabric Definition/DefType API for pattern matching.
 *
 * In fabric 1.24+, `RW.definition` returns `Definition` (which wraps `DefType` plus metadata).
 * Composite DefType variants (Opt, Arr, Obj, Poly) reference `Definition` for inner types.
 * This helper extracts the structural `DefType` for pattern matching.
 */
object DefTypeHelper {
  /**
   * Extracts the structural DefType from a Definition, suitable for pattern matching.
   */
  def unwrap(d: Definition): DefType = d.defType

  /**
   * Identity unwrap for DefType — allows call sites that already have a DefType
   * to use unwrap uniformly.
   */
  def unwrap(d: DefType): DefType = d

  /**
   * Extracts the className from a Definition.
   */
  def className(d: Definition): Option[String] = d.className

  /**
   * Extracts the inner Definition from composite DefType variants, for recursive processing.
   */
  def inner(d: Definition): Definition = d.defType match {
    case DefType.Opt(t) => t
    case DefType.Arr(t) => t
    case _ => d
  }
}
