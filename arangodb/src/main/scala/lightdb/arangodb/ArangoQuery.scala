package lightdb.arangodb

import fabric.*
import fabric.io.JsonFormatter
import lightdb.SortDirection
import lightdb.doc.{Document, DocumentModel}
import lightdb.filter.{Condition, Filter}

/**
 * Translates LightDB `Filter`/`Sort` ASTs into AQL boolean expressions over the document variable
 * `d` (and `CURRENT` inside nested array filters). Values are rendered as JSON literals — JSON is
 * valid AQL — which avoids bind-parameter plumbing.
 *
 * Semantics mirror the MongoDB backend: collection-field `has`/equality means "contains all
 * elements"; tokenized fields are stored as token arrays; nested `MustNot` is collection-scoped
 * ("no element matches").
 */
object ArangoQuery {
  /** Tokenizer shared by storage and tokenized-field equality: lowercase, split on non-alphanumerics. */
  def tokenize(s: String): List[String] =
    s.toLowerCase.split("[^\\p{L}\\p{Nd}]+").iterator.filter(_.nonEmpty).toList

  private def lit(json: Json): String = JsonFormatter.Compact(json)
  private def litStr(s: String): String = lit(str(s))

  // ArangoDB reserves the top-level attributes `_id`/`_key`/`_rev`/`_from`/`_to` (the latter two are
  // silently dropped in document collections), so LightDB's `_id` and EdgeDocument's `_from`/`_to`
  // are stored under escaped names. Filters/sorts must reference the escaped names too.
  def escapeKey(name: String): String = name match {
    case "_id" => "_key"
    case "_from" => "from_"
    case "_to" => "to_"
    case "_rev" => "rev_"
    case n => n
  }
  def unescapeKey(name: String): String = name match {
    case "_key" => "_id"
    case "from_" => "_from"
    case "to_" => "_to"
    case "rev_" => "_rev"
    case n => n
  }
  private def fieldRef(alias: String, name: String): String = s"$alias.`${escapeKey(name)}`"

  // Escape regex metacharacters so a literal substring can be used in REGEX_TEST.
  private def escapeRegex(s: String): String = s.flatMap {
    case c if "\\.^$|?*+()[]{}".contains(c) => "\\" + c
    case c => c.toString
  }

  def translate[Doc <: Document[Doc]](filter: Filter[Doc], model: DocumentModel[Doc]): String = filter match {
    case f: Filter.Equals[Doc, _] =>
      if model.fieldByName(f.fieldName).isTokenized then tokenizedEquals(f.fieldName, f.getJson(model))
      else equality(f.fieldName, f.getJson(model))
    case f: Filter.NotEquals[Doc, _] =>
      if model.fieldByName(f.fieldName).isTokenized then s"NOT (${tokenizedEquals(f.fieldName, f.getJson(model))})"
      else s"NOT (${equality(f.fieldName, f.getJson(model))})"
    case f: Filter.In[Doc, _] =>
      val values = lit(arr(f.getJson(model)*))
      if model.fieldByName(f.fieldName).isArr then s"${fieldRef("d", f.fieldName)} ANY IN $values"
      else s"${fieldRef("d", f.fieldName)} IN $values"
    case f: Filter.RangeLong[Doc] => range(fieldRef("d", f.fieldName), f.from.map(v => num(v)), f.to.map(v => num(v)))
    case f: Filter.RangeDouble[Doc] => range(fieldRef("d", f.fieldName), f.from.map(v => num(v)), f.to.map(v => num(v)))
    case f: Filter.StartsWith[Doc, _] => stringOp(f.fieldName, model, e => s"REGEX_TEST($e, ${litStr("^" + escapeRegex(f.query))})")
    case f: Filter.EndsWith[Doc, _] => stringOp(f.fieldName, model, e => s"REGEX_TEST($e, ${litStr(escapeRegex(f.query) + "$")})")
    case f: Filter.Contains[Doc, _] => stringOp(f.fieldName, model, e => s"REGEX_TEST($e, ${litStr(escapeRegex(f.query))})")
    case f: Filter.Regex[Doc, _] => stringOp(f.fieldName, model, e => s"REGEX_TEST($e, ${litStr(f.expression)})")
    case f: Filter.Exact[Doc, _] => stringOp(f.fieldName, model, e => s"$e == ${litStr(f.query)}")
    case m: Filter.Multi[Doc] => multi(m, model, translate)
    case f: Filter.Nested[Doc] => nestedClause(fieldRef("d", f.path), f.path + ".", f.filter)
    case _: Filter.MatchNone[Doc] => "false"
    case other =>
      throw new UnsupportedOperationException(s"ArangoDB backend does not yet support filter: ${other.getClass.getSimpleName}")
  }

  // Collection `has`/equality serializes the value to a JSON array ("contains all elements"); a
  // scalar value is a plain equality.
  private def equality(field: String, json: Json): String = json match {
    case Arr(values, _) =>
      if values.isEmpty then "true"
      else values.map(v => s"${lit(v)} IN ${fieldRef("d", field)}").mkString("(", " AND ", ")")
    case other => s"${fieldRef("d", field)} == ${lit(other)}"
  }

  private def tokenizedEquals(field: String, json: Json): String = {
    val toks = json match {
      case Str(s, _) => tokenize(s)
      case _ => Nil
    }
    if toks.isEmpty then "false"
    else toks.map(t => s"${litStr(t)} IN ${fieldRef("d", field)}").mkString("(", " AND ", ")")
  }

  private def range(ref: String, from: Option[Json], to: Option[Json]): String = {
    val parts = List(from.map(v => s"$ref >= ${lit(v)}"), to.map(v => s"$ref <= ${lit(v)}")).flatten
    if parts.isEmpty then "true" else parts.mkString("(", " AND ", ")")
  }

  // Applies a string predicate to a scalar field, or to ANY element of an array field.
  private def stringOp[Doc <: Document[Doc]](fieldName: String, model: DocumentModel[Doc], makePred: String => String): String =
    if model.fieldByName(fieldName).isArr then s"LENGTH(${fieldRef("d", fieldName)}[* FILTER ${makePred("CURRENT")}]) > 0"
    else makePred(fieldRef("d", fieldName))

  private def multi[Doc <: Document[Doc]](m: Filter.Multi[Doc],
                                          model: DocumentModel[Doc],
                                          tr: (Filter[Doc], DocumentModel[Doc]) => String): String = {
    val musts = m.filters.collect { case c if c.condition == Condition.Must || c.condition == Condition.Filter => tr(c.filter, model) }
    val mustNots = m.filters.collect { case c if c.condition == Condition.MustNot => s"NOT (${tr(c.filter, model)})" }
    val shoulds = m.filters.collect { case c if c.condition == Condition.Should => tr(c.filter, model) }
    val clauses = scala.collection.mutable.ListBuffer.empty[String]
    clauses ++= musts
    clauses ++= mustNots
    if shoulds.nonEmpty && m.minShould > 0 then clauses += shoulds.mkString("(", " OR ", ")")
    if clauses.isEmpty then "true" else clauses.mkString("(", " AND ", ")")
  }

  // -- Nested (AQL inline array filters) -----------------------------------------------------------
  // `arrExpr` is the AQL expression for the nested array. MustNot is collection-scoped: a positive
  // same-element match (LENGTH(...) > 0) plus, per MustNot, "no element matches" (LENGTH(...) == 0).
  private def nestedClause[Doc <: Document[Doc]](arrExpr: String, prefix: String, filter: Filter[Doc]): String = filter match {
    case m: Filter.Multi[Doc] =>
      val positive = m.filters.filter(_.condition != Condition.MustNot)
      val negatives = m.filters.collect { case c if c.condition == Condition.MustNot => c.filter }
      val pos = if positive.nonEmpty then s"LENGTH($arrExpr[* FILTER ${elementPred(prefix, Filter.Multi(m.minShould, positive))}]) > 0" else "true"
      val negs = negatives.map(nf => s"LENGTH($arrExpr[* FILTER ${elementPred(prefix, nf)}]) == 0")
      (pos :: negs).mkString("(", " AND ", ")")
    case other =>
      s"LENGTH($arrExpr[* FILTER ${elementPred(prefix, other)}]) > 0"
  }

  /** Predicate over a nested array element (`CURRENT`); inner field names are stripped of `prefix`. */
  private def elementPred[Doc <: Document[Doc]](prefix: String, filter: Filter[Doc]): String = {
    def rel(n: String): String = if n.startsWith(prefix) then n.substring(prefix.length) else n
    def ref(n: String): String = fieldRef("CURRENT", rel(n))
    filter match {
      case f: Filter.Nested[Doc] => nestedClause(fieldRef("CURRENT", rel(f.path)), f.path + ".", f.filter)
      case m: Filter.Multi[Doc] =>
        val musts = m.filters.collect { case c if c.condition == Condition.Must || c.condition == Condition.Filter => elementPred(prefix, c.filter) }
        val mustNots = m.filters.collect { case c if c.condition == Condition.MustNot => s"NOT (${elementPred(prefix, c.filter)})" }
        val shoulds = m.filters.collect { case c if c.condition == Condition.Should => elementPred(prefix, c.filter) }
        val clauses = scala.collection.mutable.ListBuffer.empty[String]
        clauses ++= musts
        clauses ++= mustNots
        if shoulds.nonEmpty && m.minShould > 0 then clauses += shoulds.mkString("(", " OR ", ")")
        if clauses.isEmpty then "true" else clauses.mkString("(", " AND ", ")")
      case f: Filter.Exact[Doc, _] => s"${ref(f.fieldName)} == ${litStr(f.query)}"
      case f: Filter.Equals[Doc, _] => s"${ref(f.fieldName)} == ${lit(anyToJson(f.value))}"
      case f: Filter.NotEquals[Doc, _] => s"${ref(f.fieldName)} != ${lit(anyToJson(f.value))}"
      case f: Filter.In[Doc, _] => s"${ref(f.fieldName)} IN ${lit(arr(f.values.map(anyToJson)*))}"
      case f: Filter.RangeLong[Doc] => range(ref(f.fieldName), f.from.map(v => num(v)), f.to.map(v => num(v)))
      case f: Filter.RangeDouble[Doc] => range(ref(f.fieldName), f.from.map(v => num(v)), f.to.map(v => num(v)))
      case f: Filter.StartsWith[Doc, _] => s"REGEX_TEST(${ref(f.fieldName)}, ${litStr("^" + escapeRegex(f.query))})"
      case f: Filter.EndsWith[Doc, _] => s"REGEX_TEST(${ref(f.fieldName)}, ${litStr(escapeRegex(f.query) + "$")})"
      case f: Filter.Contains[Doc, _] => s"REGEX_TEST(${ref(f.fieldName)}, ${litStr(escapeRegex(f.query))})"
      case f: Filter.Regex[Doc, _] => s"REGEX_TEST(${ref(f.fieldName)}, ${litStr(f.expression)})"
      case _: Filter.MatchNone[Doc] => "false"
      case other =>
        throw new UnsupportedOperationException(s"ArangoDB nested filter does not yet support: ${other.getClass.getSimpleName}")
    }
  }

  private def anyToJson(v: Any): Json = v match {
    case null => Null
    case s: String => str(s)
    case i: Int => num(i.toLong)
    case l: Long => num(l)
    case d: Double => num(d)
    case f: Float => num(f.toDouble)
    case b: Boolean => bool(b)
    case other => str(other.toString)
  }

  // -- Residual (in-memory) predicates -------------------------------------------------------------
  private def isResidualLeaf[Doc <: Document[Doc]](f: Filter[Doc]): Boolean = f match {
    case _: Filter.Distance[Doc] | _: Filter.SpatialContains[Doc] | _: Filter.SpatialIntersects[Doc] | _: Filter.DrillDownFacetFilter[Doc] => true
    case _ => false
  }

  def filterHasResidual[Doc <: Document[Doc]](f: Filter[Doc]): Boolean = f match {
    case r if isResidualLeaf(r) => true
    case m: Filter.Multi[Doc] => m.filters.exists(c => filterHasResidual(c.filter))
    case _ => false
  }

  /** Superset AQL filter dropping residual predicates (evaluated in-memory). Residual leaves -> true. */
  def translateLenient[Doc <: Document[Doc]](filter: Filter[Doc], model: DocumentModel[Doc]): String = filter match {
    case r if isResidualLeaf(r) => "true"
    case m: Filter.Multi[Doc] =>
      val musts = m.filters.collect { case c if c.condition == Condition.Must || c.condition == Condition.Filter => translateLenient(c.filter, model) }
      // A widened MustNot would wrongly exclude documents; keep only fully-translatable MustNots.
      val mustNots = m.filters.collect { case c if c.condition == Condition.MustNot && !filterHasResidual(c.filter) => s"NOT (${translate(c.filter, model)})" }
      val shoulds = m.filters.collect { case c if c.condition == Condition.Should => translateLenient(c.filter, model) }
      val clauses = scala.collection.mutable.ListBuffer.empty[String]
      clauses ++= musts
      clauses ++= mustNots
      if shoulds.nonEmpty && m.minShould > 0 then clauses += shoulds.mkString("(", " OR ", ")")
      if clauses.isEmpty then "true" else clauses.mkString("(", " AND ", ")")
    case other => translate(other, model)
  }

  /** AQL SORT clause body (without the `SORT` keyword); `None` when no field-based sort applies. */
  def sortClause(sorts: List[lightdb.Sort]): Option[String] = {
    val parts = sorts.collect {
      case bf: lightdb.Sort.ByField[_, _] =>
        s"${fieldRef("d", bf.field.name)} ${if bf.direction == SortDirection.Descending then "DESC" else "ASC"}"
      case lightdb.Sort.IndexOrder => "d.`_key` ASC" // deterministic; with LightDB's sequential ids == insertion order
    }
    if parts.isEmpty then None else Some(parts.mkString(", "))
  }
}
