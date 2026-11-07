package lightdb.lucene

import fabric.define.DefType

/**
 * An empty list is indexed as the term "[]" so the field is still present on the document, but only where the list's own values
 * are terms too. Lucene keeps one structure per field across documents, and a list of numbers or booleans is indexed as points: a
 * "[]" term on one document and points on the next fails the second insert ("Inconsistency of field data structures across
 * documents"). An empty numeric list therefore adds nothing.
 */
object EmptyListMarker {
  def wanted(element: DefType): Boolean = element match {
    case DefType.Opt(inner) => wanted(inner.defType)
    case DefType.Int | DefType.Dec | DefType.Bool => false
    case _ => true
  }
}
