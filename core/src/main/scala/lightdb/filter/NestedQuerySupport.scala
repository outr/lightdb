package lightdb.filter

import fabric.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.field.IndexingState
import lightdb.spatial.{Geo, Spatial}

import scala.util.Try

object NestedQuerySupport {
  def validateFallbackCompatible[Doc <: Document[Doc]](filter: Option[Filter[Doc]]): Unit =
    filter.foreach(validateFallbackCompatible)

  def validateFallbackCompatible[Doc <: Document[Doc]](filter: Filter[Doc]): Unit = {
    def loop(current: Filter[Doc], inNested: Boolean): Unit = current match {
      case n: Filter.Nested[Doc] =>
        loop(n.filter, inNested = true)
      case _: Filter.Distance[Doc] if inNested =>
        throw new UnsupportedOperationException("Nested fallback does not support Distance inside nested filters")
      case _: Filter.DrillDownFacetFilter[Doc] if inNested =>
        throw new UnsupportedOperationException("Nested fallback does not support DrillDownFacetFilter inside nested filters")
      case _: Filter.ExistsChild[Doc @unchecked] if inNested =>
        throw new UnsupportedOperationException("Nested fallback does not support ExistsChild inside nested filters")
      case _: Filter.ChildConstraints[Doc @unchecked] if inNested =>
        throw new UnsupportedOperationException("Nested fallback does not support ChildConstraints inside nested filters")
      case m: Filter.Multi[Doc] =>
        m.filters.foreach(fc => loop(fc.filter, inNested))
      case _ =>
    }
    loop(filter, inNested = false)
  }

  def containsNested[Doc <: Document[Doc]](filter: Option[Filter[Doc]]): Boolean =
    filter.exists(containsNested)

  def containsNested[Doc <: Document[Doc]](filter: Filter[Doc]): Boolean = filter match {
    case _: Filter.Nested[Doc] => true
    case m: Filter.Multi[Doc] => m.filters.exists(fc => containsNested(fc.filter))
    case _ => false
  }

  /**
   * Produces a broad/superset filter by removing nested constraints.
   * Useful for capability-based fallback execution where nested predicates are evaluated post-query.
   */
  def stripNested[Doc <: Document[Doc]](filterOpt: Option[Filter[Doc]]): Option[Filter[Doc]] =
    filterOpt.flatMap(stripNested)

  def stripNested[Doc <: Document[Doc]](filter: Filter[Doc]): Option[Filter[Doc]] = filter match {
    case _: Filter.Nested[Doc] =>
      None
    case m: Filter.Multi[Doc] =>
      val clauses = m.filters.flatMap { fc =>
        stripNested(fc.filter).map(f => fc.copy(filter = f))
      }
      if clauses.isEmpty then None else Some(m.copy(filters = clauses))
    case other =>
      Some(other)
  }

  def eval[Doc <: Document[Doc], Model <: DocumentModel[Doc]](filter: Filter[Doc], model: Model, doc: Doc): Boolean = {
    val state = new IndexingState
    val docJson = model.rw.read(doc)
    evalFilter(filter, model, doc, docJson, state)
  }

  private def evalFilter[Doc <: Document[Doc], Model <: DocumentModel[Doc]](filter: Filter[Doc],
                                                                             model: Model,
                                                                             doc: Doc,
                                                                             docJson: Json,
                                                                             state: IndexingState): Boolean = filter match {
    case _: Filter.MatchNone[Doc] => false
    case f: Filter.Nested[Doc] =>
      val elements = nestedElements(docJson, f.path)
      elements.exists(el => evalNestedInner(f.filter, f.path, el))
    case f: Filter.Equals[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      val value = field.get(doc, field, state)
      equalsValue(value, f.value)
    case f: Filter.NotEquals[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      val value = field.get(doc, field, state)
      !equalsValue(value, f.value)
    case f: Filter.In[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      val value = field.get(doc, field, state)
      iterable(value) match {
        case Some(values) => values.exists(v => f.values.contains(v))
        case None => f.values.contains(value)
      }
    case f: Filter.RangeLong[Doc] =>
      val value = model.fieldByName[Any](f.fieldName).asInstanceOf[Field[Doc, Any]].get(doc, model.fieldByName[Any](f.fieldName).asInstanceOf[Field[Doc, Any]], state)
      toLong(value).exists(v => f.from.forall(v >= _) && f.to.forall(v <= _))
    case f: Filter.RangeDouble[Doc] =>
      val value = model.fieldByName[Any](f.fieldName).asInstanceOf[Field[Doc, Any]].get(doc, model.fieldByName[Any](f.fieldName).asInstanceOf[Field[Doc, Any]], state)
      toDouble(value).exists(v => f.from.forall(v >= _) && f.to.forall(v <= _))
    case f: Filter.StartsWith[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      matchesAny(field.get(doc, field, state), _.startsWith(f.query))
    case f: Filter.EndsWith[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      matchesAny(field.get(doc, field, state), _.endsWith(f.query))
    case f: Filter.Contains[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      matchesAny(field.get(doc, field, state), _.contains(f.query))
    case f: Filter.Exact[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      matchesAny(field.get(doc, field, state), _ == f.query)
    case f: Filter.Regex[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      def re(s: String): Boolean = Try(f.expression.r.findFirstIn(s).nonEmpty).getOrElse(false)
      matchesAny(field.get(doc, field, state), re)
    case f: Filter.Distance[Doc] =>
      val field = model.fieldByName[Any](f.fieldName).asInstanceOf[Field[Doc, Any]]
      val value = field.get(doc, field, state)
      val geos = iterable(value).map(_.collect { case g: Geo => g }.toList).getOrElse(value match {
        case g: Geo => List(g)
        case _ => Nil
      })
      geos.exists(g => Spatial.distance(g, f.from).valueInMeters <= f.radius.valueInMeters)
    case f: Filter.DrillDownFacetFilter[Doc] =>
      val field = model.fieldByName[List[lightdb.facet.FacetValue]](f.fieldName).asInstanceOf[Field[Doc, List[lightdb.facet.FacetValue]]]
      val values = field.get(doc, field, state)
      values.exists { fv =>
        if f.showOnlyThisLevel then fv.path == f.path else fv.path.startsWith(f.path)
      }
    case _: Filter.ExistsChild[Doc @unchecked] =>
      throw new UnsupportedOperationException("ExistsChild must be resolved before nested fallback evaluation")
    case _: Filter.ChildConstraints[Doc @unchecked] =>
      throw new UnsupportedOperationException("ChildConstraints must be resolved before nested fallback evaluation")
    case m: Filter.Multi[Doc] =>
      val must = m.filters.filter(c => c.condition == Condition.Must || c.condition == Condition.Filter).map(_.filter)
      val mustNot = m.filters.filter(_.condition == Condition.MustNot).map(_.filter)
      val should = m.filters.filter(_.condition == Condition.Should).map(_.filter)
      val mustOk = must.forall(f => evalFilter(f, model, doc, docJson, state))
      val mustNotOk = mustNot.forall(f => !evalFilter(f, model, doc, docJson, state))
      val shouldCount = should.count(f => evalFilter(f, model, doc, docJson, state))
      val requiredShould = if should.nonEmpty then m.minShould else 0
      mustOk && mustNotOk && shouldCount >= requiredShould
  }

  private def evalNestedInner[Doc <: Document[Doc]](filter: Filter[Doc], nestedPath: String, elementJson: Json): Boolean = {
    def normalize(name: String): String = {
      val prefix = s"$nestedPath."
      if name.startsWith(prefix) then name.substring(prefix.length)
      else name
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
      val values = descend(List(elementJson), segments).flatMap {
        case a: Arr => a.value.toList
        case other => List(other)
      }
      values
    }

    def evalLocal(f: Filter[Doc]): Boolean = f match {
      case _: Filter.MatchNone[Doc] => false
      case f: Filter.Nested[Doc] =>
        // Nested-in-nested: evaluate against nested descendants of this element.
        nestedElements(elementJson, f.path).exists(el => evalNestedInner(f.filter, f.path, el))
      case f: Filter.Equals[Doc, _] =>
        equalsJson(valueForField(f.fieldName), f.value)
      case f: Filter.NotEquals[Doc, _] =>
        !equalsJson(valueForField(f.fieldName), f.value)
      case f: Filter.In[Doc, _] =>
        val set = f.values.toSet
        valueForField(f.fieldName).exists(j => set.exists(v => jsonEqualsAny(j, v)))
      case f: Filter.RangeLong[Doc] =>
        valueForField(f.fieldName).flatMap(j => toLong(j)).exists(v => f.from.forall(v >= _) && f.to.forall(v <= _))
      case f: Filter.RangeDouble[Doc] =>
        valueForField(f.fieldName).flatMap(j => toDouble(j)).exists(v => f.from.forall(v >= _) && f.to.forall(v <= _))
      case f: Filter.StartsWith[Doc, _] =>
        valueForField(f.fieldName).exists(j => jsonString(j).startsWith(f.query))
      case f: Filter.EndsWith[Doc, _] =>
        valueForField(f.fieldName).exists(j => jsonString(j).endsWith(f.query))
      case f: Filter.Contains[Doc, _] =>
        valueForField(f.fieldName).exists(j => jsonString(j).contains(f.query))
      case f: Filter.Exact[Doc, _] =>
        valueForField(f.fieldName).exists(j => jsonString(j) == f.query)
      case f: Filter.Regex[Doc, _] =>
        valueForField(f.fieldName).exists(j => Try(f.expression.r.findFirstIn(jsonString(j)).nonEmpty).getOrElse(false))
      case _: Filter.Distance[Doc] =>
        throw new UnsupportedOperationException("Nested fallback does not support Distance inside nested filters")
      case _: Filter.DrillDownFacetFilter[Doc] =>
        throw new UnsupportedOperationException("Nested fallback does not support DrillDownFacetFilter inside nested filters")
      case _: Filter.ExistsChild[Doc @unchecked] =>
        throw new UnsupportedOperationException("Nested fallback does not support ExistsChild inside nested filters")
      case _: Filter.ChildConstraints[Doc @unchecked] =>
        throw new UnsupportedOperationException("Nested fallback does not support ChildConstraints inside nested filters")
      case m: Filter.Multi[Doc] =>
        val must = m.filters.filter(c => c.condition == Condition.Must || c.condition == Condition.Filter).map(_.filter)
        val mustNot = m.filters.filter(_.condition == Condition.MustNot).map(_.filter)
        val should = m.filters.filter(_.condition == Condition.Should).map(_.filter)
        val mustOk = must.forall(evalLocal)
        val mustNotOk = mustNot.forall(f => !evalLocal(f))
        val shouldCount = should.count(evalLocal)
        val requiredShould = if should.nonEmpty then m.minShould else 0
        mustOk && mustNotOk && shouldCount >= requiredShould
    }

    evalLocal(filter)
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

  private def equalsJson(values: List[Json], expected: Any): Boolean = {
    if values.isEmpty then {
      expected == null || expected == None
    } else {
      expected match {
        case iterableExpected: Iterable[?] =>
          iterableExpected.forall(ev => values.exists(v => jsonEqualsAny(v, ev)))
        case arrayExpected: Array[?] =>
          arrayExpected.forall(ev => values.exists(v => jsonEqualsAny(v, ev)))
        case other =>
          values.exists(v => jsonEqualsAny(v, other))
      }
    }
  }

  private def jsonEqualsAny(json: Json, expected: Any): Boolean = {
    (json, expected) match {
      case (Str(s, _), e: String) => s == e
      case (NumInt(l, _), e: Byte) => l == e.toLong
      case (NumInt(l, _), e: Short) => l == e.toLong
      case (NumInt(l, _), e: Int) => l == e.toLong
      case (NumInt(l, _), e: Long) => l == e
      case (NumInt(l, _), e: Float) => l.toDouble == e.toDouble
      case (NumInt(l, _), e: Double) => l.toDouble == e
      case (NumDec(d, _), e: Byte) => d.toDouble == e.toDouble
      case (NumDec(d, _), e: Short) => d.toDouble == e.toDouble
      case (NumDec(d, _), e: Int) => d.toDouble == e.toDouble
      case (NumDec(d, _), e: Long) => d.toDouble == e.toDouble
      case (NumDec(d, _), e: Float) => d.toDouble == e.toDouble
      case (NumDec(d, _), e: Double) => d.toDouble == e
      case (Bool(b, _), e: Boolean) => b == e
      case (Null, null) => true
      case (Null, None) => true
      case (_, Some(v)) => jsonEqualsAny(json, v)
      case (_, None) => json == Null
      case _ => json.toString == Option(expected).map(_.toString).getOrElse("null")
    }
  }

  private def equalsValue(actual: Any, expected: Any): Boolean = iterable(actual) match {
    case Some(values) =>
      iterable(expected) match {
        case Some(req) => req.forall(values.toSet.contains)
        case None => values.toSet.contains(expected)
      }
    case None =>
      actual == expected
  }

  private def iterable(value: Any): Option[Iterable[Any]] = value match {
    case null => None
    case it: Iterable[?] => Some(it.asInstanceOf[Iterable[Any]])
    case arr: Array[?] => Some(arr.toIndexedSeq.asInstanceOf[IndexedSeq[Any]])
    case jc: java.util.Collection[?] => Some(jc.toArray.toIndexedSeq.asInstanceOf[IndexedSeq[Any]])
    case _ => None
  }

  private def matchesAny(value: Any, p: String => Boolean): Boolean = iterable(value) match {
    case Some(values) => values.exists(v => p(Option(v).map(_.toString).getOrElse("")))
    case None => p(Option(value).map(_.toString).getOrElse(""))
  }

  private def jsonString(json: Json): String = json match {
    case Str(s, _) => s
    case NumInt(l, _) => l.toString
    case NumDec(d, _) => d.toString
    case Bool(b, _) => b.toString
    case Null => "null"
    case other => other.toString
  }

  private def toLong(value: Any): Option[Long] = value match {
    case null => None
    case Some(v) => toLong(v)
    case None => None
    case n: java.lang.Number => Some(n.longValue())
    case s: String => s.toLongOption
    case NumInt(l, _) => Some(l)
    case NumDec(d, _) => Some(d.toLong)
    case _ => None
  }

  private def toDouble(value: Any): Option[Double] = value match {
    case null => None
    case Some(v) => toDouble(v)
    case None => None
    case n: java.lang.Number => Some(n.doubleValue())
    case s: String => s.toDoubleOption
    case NumInt(l, _) => Some(l.toDouble)
    case NumDec(d, _) => Some(d.toDouble)
    case _ => None
  }
}

