package lightdb.tantivy

import fabric.*
import fabric.rw.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.filter.*

/** Evaluates a `Filter` against a doc with **block-level `must_not`** semantics inside nested
 *  filters — i.e., a `Filter.Multi` with `MustNot` clauses inside a `Filter.Nested(path, ...)`
 *  rejects the parent if ANY element under `path` matches the `MustNot` clause, not just the
 *  candidate element that satisfies the `Must`/`Should` clauses.
 *
 *  This matches Lucene's block-join semantics, which `NestedQuerySupport.eval` (the default
 *  shared evaluator) does not implement — that one applies `MustNot` per-element.
 *
 *  Falls back to `NestedQuerySupport.eval` for non-nested or trivial cases.
 */
private[tantivy] object TantivyNestedEval {

  def eval[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    filter: Filter[Doc],
    model: Model,
    doc: Doc
  ): Boolean = {
    val docJson = model.rw.read(doc)
    evalOuter(filter, model, doc, docJson)
  }

  private def evalOuter[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    filter: Filter[Doc],
    model: Model,
    doc: Doc,
    docJson: Json
  ): Boolean = filter match {
    case n: Filter.Nested[Doc] =>
      val elements = nestedElements(docJson, n.path)
      evalNestedBlock(n.filter, n.path, elements)
    case m: Filter.Multi[Doc] =>
      val must = m.filters.filter(c => c.condition == Condition.Must || c.condition == Condition.Filter).map(_.filter)
      val mustNot = m.filters.filter(_.condition == Condition.MustNot).map(_.filter)
      val should = m.filters.filter(_.condition == Condition.Should).map(_.filter)
      val mustOk = must.forall(f => evalOuter(f, model, doc, docJson))
      val mustNotOk = mustNot.forall(f => !evalOuter(f, model, doc, docJson))
      val shouldCount = should.count(f => evalOuter(f, model, doc, docJson))
      val requiredShould = if should.nonEmpty then m.minShould else 0
      mustOk && mustNotOk && shouldCount >= requiredShould
    case other =>
      // Non-nested clauses defer to the shared evaluator.
      NestedQuerySupport.eval(other, model, doc)
  }

  /** Block-level evaluation against the list of nested elements.
   *
   *  - For `Multi(must, should, mustNot)`: at least one element satisfies must+should,
   *    AND no element matches any mustNot clause.
   *  - For non-Multi inner: at least one element matches the filter (per-element).
   *  - Recursive for nested-in-nested.
   */
  private def evalNestedBlock[Doc <: Document[Doc]](
    filter: Filter[Doc],
    path: String,
    elements: List[Json]
  ): Boolean = filter match {
    case _: Filter.MatchNone[Doc] => false
    case m: Filter.Multi[Doc] =>
      val must = m.filters.filter(c => c.condition == Condition.Must || c.condition == Condition.Filter).map(_.filter)
      val mustNot = m.filters.filter(_.condition == Condition.MustNot).map(_.filter)
      val should = m.filters.filter(_.condition == Condition.Should).map(_.filter)
      val noneOfMustNot = elements.forall(el => !mustNot.exists(f => evalElement(f, path, el)))
      if !noneOfMustNot then false
      else {
        // Find an element matching must AND should (per minShould).
        elements.exists { el =>
          val mustOk = must.forall(f => evalElement(f, path, el))
          val shouldCount = should.count(f => evalElement(f, path, el))
          val requiredShould = if should.nonEmpty then m.minShould else 0
          mustOk && shouldCount >= requiredShould
        }
      }
    case nested: Filter.Nested[Doc] =>
      // Nested-in-nested: each outer element has a deeper element list at the inner path.
      elements.exists { el =>
        val deeperElements = nestedElements(el, nested.path)
        evalNestedBlock(nested.filter, nested.path, deeperElements)
      }
    case other =>
      // Single non-Multi clause: at least one element matches.
      elements.exists(el => evalElement(other, path, el))
  }

  /** Evaluate a single (possibly nested) clause against a single nested element. Mirrors the
   *  per-element evaluation in `NestedQuerySupport.evalNestedInner`.
   */
  private def evalElement[Doc <: Document[Doc]](
    filter: Filter[Doc],
    path: String,
    elementJson: Json
  ): Boolean = filter match {
    case nested: Filter.Nested[Doc] =>
      val deeperElements = nestedElements(elementJson, nested.path)
      evalNestedBlock(nested.filter, nested.path, deeperElements)
    case m: Filter.Multi[Doc] =>
      val must = m.filters.filter(c => c.condition == Condition.Must || c.condition == Condition.Filter).map(_.filter)
      val mustNot = m.filters.filter(_.condition == Condition.MustNot).map(_.filter)
      val should = m.filters.filter(_.condition == Condition.Should).map(_.filter)
      val mustOk = must.forall(f => evalElement(f, path, elementJson))
      val mustNotOk = mustNot.forall(f => !evalElement(f, path, elementJson))
      val shouldCount = should.count(f => evalElement(f, path, elementJson))
      val requiredShould = if should.nonEmpty then m.minShould else 0
      mustOk && mustNotOk && shouldCount >= requiredShould
    case other =>
      // Delegate the leaf-level filter evaluation to the shared library by synthesizing a
      // single-element document. This preserves all the value-coercion logic (numeric, string,
      // null, etc.) without duplicating it.
      evalLeafAgainstElement(other, path, elementJson)
  }

  /** Use `NestedQuerySupport.eval` semantics for leaf filters by walking the element fields. */
  private def evalLeafAgainstElement[Doc <: Document[Doc]](
    filter: Filter[Doc],
    nestedPath: String,
    elementJson: Json
  ): Boolean = {
    def normalize(name: String): String = {
      val prefix = s"$nestedPath."
      if name.startsWith(prefix) then name.substring(prefix.length) else name
    }

    def valueForField(fieldName: String): List[Json] = {
      val segments = normalize(fieldName).split("\\.").toList.filter(_.nonEmpty)
      def descend(nodes: List[Json], remaining: List[String]): List[Json] = remaining match {
        case Nil => nodes
        case h :: t =>
          val next = nodes.flatMap {
            case o: Obj => o.value.get(h).toList
            case a: Arr => a.value.toList.flatMap {
              case oo: Obj => oo.value.get(h).toList
              case _ => Nil
            }
            case _ => Nil
          }
          descend(next, t)
      }
      descend(List(elementJson), segments).flatMap {
        case a: Arr => a.value.toList
        case other => List(other)
      }
    }

    filter match {
      case f: Filter.Equals[Doc, _] =>
        valueForField(f.fieldName).exists(j => jsonEqualsAny(j, f.value))
      case f: Filter.NotEquals[Doc, _] =>
        !valueForField(f.fieldName).exists(j => jsonEqualsAny(j, f.value))
      case f: Filter.RangeLong[Doc] =>
        valueForField(f.fieldName).flatMap(toLong).exists(v => f.from.forall(v >= _) && f.to.forall(v <= _))
      case f: Filter.RangeDouble[Doc] =>
        valueForField(f.fieldName).flatMap(toDouble).exists(v => f.from.forall(v >= _) && f.to.forall(v <= _))
      case f: Filter.StartsWith[Doc, _] =>
        valueForField(f.fieldName).exists(j => jsonString(j).startsWith(f.query))
      case f: Filter.EndsWith[Doc, _] =>
        valueForField(f.fieldName).exists(j => jsonString(j).endsWith(f.query))
      case f: Filter.Contains[Doc, _] =>
        valueForField(f.fieldName).exists(j => jsonString(j).contains(f.query))
      case f: Filter.Exact[Doc, _] =>
        valueForField(f.fieldName).exists(j => jsonString(j) == f.query)
      case f: Filter.In[Doc, _] =>
        valueForField(f.fieldName).exists(j => f.values.exists(v => jsonEqualsAny(j, v)))
      case _: Filter.MatchNone[Doc] => false
      case _ =>
        // Spatial/facet/etc. — delegate via a minimal stub. Spec barriers above prevent these.
        throw new UnsupportedOperationException(
          s"TantivyNestedEval: leaf filter ${filter.getClass.getSimpleName} not supported inside nested clauses"
        )
    }
  }

  private def nestedElements(docJson: Json, path: String): List[Json] = {
    val segments = path.split("\\.").toList.filter(_.nonEmpty)
    if segments.isEmpty then Nil
    else {
      def descend(nodes: List[Json], remaining: List[String]): List[Json] = remaining match {
        case Nil => nodes
        case h :: t =>
          val next = nodes.flatMap {
            case o: Obj => o.value.get(h).toList
            case a: Arr => a.value.toList.flatMap {
              case oo: Obj => oo.value.get(h).toList
              case _ => Nil
            }
            case _ => Nil
          }
          descend(next, t)
      }
      descend(List(docJson), segments).flatMap {
        case a: Arr => a.value.toList
        case other => List(other)
      }.filter(_.isObj)
    }
  }

  private def jsonEqualsAny(json: Json, expected: Any): Boolean = (json, expected) match {
    case (Str(s, _), e: String) => s == e
    case (NumInt(l, _), e: Int) => l == e.toLong
    case (NumInt(l, _), e: Long) => l == e
    case (NumInt(l, _), e: Double) => l.toDouble == e
    case (NumDec(d, _), e: Int) => d.toDouble == e.toDouble
    case (NumDec(d, _), e: Long) => d.toDouble == e.toDouble
    case (NumDec(d, _), e: Double) => d.toDouble == e
    case (NumDec(d, _), e: Float) => d.toDouble == e.toDouble
    case (Bool(b, _), e: Boolean) => b == e
    case (Null, null | None) => true
    case (_, Some(v)) => jsonEqualsAny(json, v)
    case (_, None) => json == Null
    case _ => json.toString == Option(expected).map(_.toString).getOrElse("null")
  }

  private def jsonString(json: Json): String = json match {
    case Str(s, _) => s
    case NumInt(l, _) => l.toString
    case NumDec(d, _) => d.toString
    case Bool(b, _) => b.toString
    case Null => "null"
    case other => other.toString
  }

  private def toLong(json: Json): Option[Long] = json match {
    case NumInt(l, _) => Some(l)
    case NumDec(d, _) => Some(d.toLong)
    case Str(s, _) => s.toLongOption
    case _ => None
  }

  private def toDouble(json: Json): Option[Double] = json match {
    case NumInt(l, _) => Some(l.toDouble)
    case NumDec(d, _) => Some(d.toDouble)
    case Str(s, _) => s.toDoubleOption
    case _ => None
  }
}
