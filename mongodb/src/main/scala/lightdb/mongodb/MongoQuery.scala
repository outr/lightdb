package lightdb.mongodb

import com.mongodb.client.model.Filters
import fabric.*
import lightdb.SortDirection
import lightdb.doc.{Document, DocumentModel}
import lightdb.filter.{Condition, Filter}
import org.bson.Document as BsonDoc
import org.bson.conversions.Bson

import java.util.regex.Pattern
import scala.jdk.CollectionConverters.*

/**
 * Translates LightDB `Filter`/`Sort` ASTs into MongoDB query/sort BSON.
 *
 * Notes on semantics:
 * - Array/collection fields rely on MongoDB's native "match any element" behavior: `eq`, `in`, and
 *   `regex` against an array field match if any element matches — so `has`/`hasAny`/`contains`/
 *   `startsWith` on a `Set`/`List` field map directly.
 * - Tokenized fields are stored as token arrays (see [[MongoDBTransaction.toBson]]); equality on a
 *   tokenized field is therefore an AND over the query's tokens rather than whole-string equality.
 */
object MongoQuery {
  /** Tokenizer shared by storage and tokenized-field equality: lowercase, split on non-alphanumerics. */
  def tokenize(s: String): List[String] =
    s.toLowerCase.split("[^\\p{L}\\p{Nd}]+").iterator.filter(_.nonEmpty).toList

  /** Convert a fabric Json value into a MongoDB-compatible Java value for filter construction. */
  def jsonToBson(json: Json): Any = json match {
    case Str(s, _) => s
    case NumInt(l, _) => java.lang.Long.valueOf(l)
    case NumDec(d, _) => java.lang.Double.valueOf(d.toDouble)
    case Bool(b, _) => java.lang.Boolean.valueOf(b)
    case Null => null
    case Arr(v, _) => v.map(jsonToBson).asJava
    case o: Obj => o.value.foldLeft(new BsonDoc()) { case (d, (k, v)) => d.append(k, jsonToBson(v)) }
  }

  def translate[Doc <: Document[Doc]](filter: Filter[Doc], model: DocumentModel[Doc]): Bson = filter match {
    case f: Filter.Equals[Doc, _] =>
      if model.fieldByName(f.fieldName).isTokenized then tokenizedEquals(f.fieldName, f.getJson(model))
      else equality(f.fieldName, f.getJson(model))
    case f: Filter.NotEquals[Doc, _] =>
      if model.fieldByName(f.fieldName).isTokenized then Filters.nor(tokenizedEquals(f.fieldName, f.getJson(model)))
      else Filters.not(equality(f.fieldName, f.getJson(model)))
    case f: Filter.In[Doc, _] =>
      Filters.in(f.fieldName, f.getJson(model).map(jsonToBson).asJava)
    case f: Filter.RangeLong[Doc] =>
      range(f.fieldName, f.from.map(java.lang.Long.valueOf), f.to.map(java.lang.Long.valueOf))
    case f: Filter.RangeDouble[Doc] =>
      range(f.fieldName, f.from.map(java.lang.Double.valueOf), f.to.map(java.lang.Double.valueOf))
    case f: Filter.StartsWith[Doc, _] => Filters.regex(f.fieldName, "^" + Pattern.quote(f.query))
    case f: Filter.EndsWith[Doc, _] => Filters.regex(f.fieldName, Pattern.quote(f.query) + "$")
    case f: Filter.Contains[Doc, _] => Filters.regex(f.fieldName, Pattern.quote(f.query))
    case f: Filter.Exact[Doc, _] => Filters.eq(f.fieldName, f.query)
    case f: Filter.Regex[Doc, _] => Filters.regex(f.fieldName, f.expression)
    case f: Filter.Multi[Doc] => multi(f, model)
    case f: Filter.Nested[Doc] => nestedClause("", f.path, f.filter)
    case _: Filter.MatchNone[Doc] => Filters.exists("_id", false) // _id always exists -> matches nothing
    case other =>
      throw new UnsupportedOperationException(s"MongoDB backend does not yet support filter: ${other.getClass.getSimpleName}")
  }

  /**
   * Translates a nested filter's inner predicate for `$elemMatch`. Inner field names are stored
   * qualified (e.g. `attrs.key`, set by `NestedAccessRuntime`); `$elemMatch` matches against array
   * elements, so we strip the `path.` prefix to reference the sub-field relatively. All inner
   * predicates of a `Multi` thus apply to the SAME element — exactly `NestedSemantics.SameElementAll`.
   */
  /**
   * Translates a `Filter.Nested(path, filter)` appearing at a scope whose element prefix is
   * `outerPrefix` (`""` at the document root). MustNot clauses use collection-scoped semantics
   * ("no element matches"), so a nested `Multi` becomes a positive same-element `$elemMatch` for its
   * Must/Should clauses plus a `$not`/`$elemMatch` per MustNot, combined with `$and` at this scope.
   */
  private def nestedClause[Doc <: Document[Doc]](outerPrefix: String, path: String, filter: Filter[Doc]): Bson = {
    val field = if path.startsWith(outerPrefix) then path.substring(outerPrefix.length) else path
    val innerPrefix = path + "."
    filter match {
      case m: Filter.Multi[Doc] =>
        val positive = m.filters.filter(_.condition != Condition.MustNot)
        val negatives = m.filters.collect { case c if c.condition == Condition.MustNot => c.filter }
        val clauses = scala.collection.mutable.ListBuffer.empty[Bson]
        if positive.nonEmpty then clauses += Filters.elemMatch(field, nestedElementPredicate(innerPrefix, Filter.Multi(m.minShould, positive)))
        negatives.foreach(nf => clauses += Filters.not(Filters.elemMatch(field, nestedElementPredicate(innerPrefix, nf))))
        if clauses.isEmpty then Filters.elemMatch(field, new BsonDoc())
        else if clauses.size == 1 then clauses.head
        else Filters.and(clauses.asJava)
      case other =>
        Filters.elemMatch(field, nestedElementPredicate(innerPrefix, other))
    }
  }

  /** A predicate on a single element of the nested array at `prefix` (== the array path + "."). */
  private def nestedElementPredicate[Doc <: Document[Doc]](prefix: String, filter: Filter[Doc]): Bson = {
    def rel(n: String): String = if n.startsWith(prefix) then n.substring(prefix.length) else n
    filter match {
      case f: Filter.Nested[Doc] => nestedClause(prefix, f.path, f.filter)
      case m: Filter.Multi[Doc] =>
        val musts = m.filters.collect { case c if c.condition == Condition.Must || c.condition == Condition.Filter => nestedElementPredicate(prefix, c.filter) }
        val mustNots = m.filters.collect { case c if c.condition == Condition.MustNot => nestedElementPredicate(prefix, c.filter) }
        val shoulds = m.filters.collect { case c if c.condition == Condition.Should => nestedElementPredicate(prefix, c.filter) }
        val clauses = scala.collection.mutable.ListBuffer.empty[Bson]
        musts.foreach(clauses += _)
        mustNots.foreach(b => clauses += Filters.nor(b))
        if shoulds.nonEmpty && m.minShould > 0 then clauses += Filters.or(shoulds.asJava)
        if clauses.isEmpty then Filters.empty() else Filters.and(clauses.asJava)
      case f: Filter.Exact[Doc, _] => Filters.eq(rel(f.fieldName), f.query)
      case f: Filter.Equals[Doc, _] => Filters.eq(rel(f.fieldName), anyToBson(f.value))
      case f: Filter.NotEquals[Doc, _] => Filters.ne(rel(f.fieldName), anyToBson(f.value))
      case f: Filter.In[Doc, _] => Filters.in(rel(f.fieldName), f.values.map(anyToBson).asJava)
      case f: Filter.RangeLong[Doc] => range(rel(f.fieldName), f.from.map(java.lang.Long.valueOf), f.to.map(java.lang.Long.valueOf))
      case f: Filter.RangeDouble[Doc] => range(rel(f.fieldName), f.from.map(java.lang.Double.valueOf), f.to.map(java.lang.Double.valueOf))
      case f: Filter.StartsWith[Doc, _] => Filters.regex(rel(f.fieldName), "^" + Pattern.quote(f.query))
      case f: Filter.EndsWith[Doc, _] => Filters.regex(rel(f.fieldName), Pattern.quote(f.query) + "$")
      case f: Filter.Contains[Doc, _] => Filters.regex(rel(f.fieldName), Pattern.quote(f.query))
      case f: Filter.Regex[Doc, _] => Filters.regex(rel(f.fieldName), f.expression)
      case _: Filter.MatchNone[Doc] => Filters.exists("__never__", true)
      case other =>
        throw new UnsupportedOperationException(s"MongoDB nested filter does not yet support: ${other.getClass.getSimpleName}")
    }
  }

  private def anyToBson(v: Any): Any = v match {
    case null => null
    case s: String => s
    case i: Int => java.lang.Long.valueOf(i.toLong)
    case l: Long => java.lang.Long.valueOf(l)
    case d: Double => java.lang.Double.valueOf(d)
    case f: Float => java.lang.Double.valueOf(f.toDouble)
    case b: Boolean => java.lang.Boolean.valueOf(b)
    case other => other.toString
  }

  // A collection field's `has`/equality serializes its value to a JSON array (e.g. `has("x")` ->
  // `["x"]`), which in LightDB means "the field contains all these elements" -> MongoDB `$all`
  // (which also matches array-element membership). A scalar value is a plain equality.
  private def equality(field: String, json: Json): Bson = json match {
    case Arr(values, _) => Filters.all(field, values.map(jsonToBson).asJava)
    case other => Filters.eq(field, jsonToBson(other))
  }

  private def tokenizedEquals(field: String, json: Json): Bson = {
    val toks = json match {
      case Str(s, _) => tokenize(s)
      case _ => Nil
    }
    if toks.isEmpty then Filters.exists("_id", false)
    else if toks.size == 1 then Filters.eq(field, toks.head)
    else Filters.and(toks.map(t => Filters.eq(field, t)).asJava)
  }

  private def range(field: String, from: Option[Any], to: Option[Any]): Bson = {
    val parts = List(from.map(v => Filters.gte(field, v)), to.map(v => Filters.lte(field, v))).flatten
    if parts.isEmpty then Filters.exists(field) else Filters.and(parts.asJava)
  }

  private def multi[Doc <: Document[Doc]](m: Filter.Multi[Doc], model: DocumentModel[Doc]): Bson =
    combineClauses(m, model, translate)

  private def combineClauses[Doc <: Document[Doc]](m: Filter.Multi[Doc],
                                                   model: DocumentModel[Doc],
                                                   tr: (Filter[Doc], DocumentModel[Doc]) => Bson): Bson = {
    val musts = m.filters.collect { case c if c.condition == Condition.Must || c.condition == Condition.Filter => tr(c.filter, model) }
    val mustNots = m.filters.collect { case c if c.condition == Condition.MustNot => tr(c.filter, model) }
    val shoulds = m.filters.collect { case c if c.condition == Condition.Should => tr(c.filter, model) }

    val clauses = scala.collection.mutable.ListBuffer.empty[Bson]
    musts.foreach(clauses += _)
    mustNots.foreach(b => clauses += Filters.nor(b))
    if shoulds.nonEmpty && m.minShould > 0 then {
      // MongoDB has no "minimum should match"; minShould == 1 is a plain OR. Higher thresholds are
      // approximated as OR (best-effort) — only reachable via the Filter.Builder surface.
      clauses += Filters.or(shoulds.asJava)
    }
    if clauses.isEmpty then Filters.empty() else Filters.and(clauses.asJava)
  }

  /** Filters MongoDB can't express, evaluated in-memory instead (see MongoDBTransaction). */
  private def isResidualLeaf[Doc <: Document[Doc]](f: Filter[Doc]): Boolean = f match {
    case _: Filter.Distance[Doc] | _: Filter.SpatialContains[Doc] | _: Filter.SpatialIntersects[Doc] | _: Filter.DrillDownFacetFilter[Doc] => true
    case _ => false
  }

  def filterHasResidual[Doc <: Document[Doc]](f: Filter[Doc]): Boolean = f match {
    case r if isResidualLeaf(r) => true
    case m: Filter.Multi[Doc] => m.filters.exists(c => filterHasResidual(c.filter))
    case _ => false
  }

  /**
   * A superset BSON filter that drops residual (in-memory-only) predicates — used to narrow the
   * candidate set before exact in-memory evaluation. Residual leaves become match-all; in an AND
   * context this is a true superset, and OR/Should contexts still over-approximate safely.
   */
  def translateLenient[Doc <: Document[Doc]](filter: Filter[Doc], model: DocumentModel[Doc]): Bson = filter match {
    case r if isResidualLeaf(r) => Filters.empty()
    case m: Filter.Multi[Doc] =>
      val musts = m.filters.collect { case c if c.condition == Condition.Must || c.condition == Condition.Filter => translateLenient(c.filter, model) }
      // A widened MustNot would wrongly exclude documents we can't actually evaluate at the DB level,
      // so only keep MustNot clauses that translate exactly (no residual); the rest are enforced in-memory.
      val mustNots = m.filters.collect { case c if c.condition == Condition.MustNot && !filterHasResidual(c.filter) => translate(c.filter, model) }
      val shoulds = m.filters.collect { case c if c.condition == Condition.Should => translateLenient(c.filter, model) }
      val clauses = scala.collection.mutable.ListBuffer.empty[Bson]
      musts.foreach(clauses += _)
      mustNots.foreach(b => clauses += Filters.nor(b))
      if shoulds.nonEmpty && m.minShould > 0 then clauses += Filters.or(shoulds.asJava)
      if clauses.isEmpty then Filters.empty() else Filters.and(clauses.asJava)
    case other => translate(other, model)
  }

  /** Build a MongoDB sort document; `None` when no field-based sort applies. */
  def sortDoc(sorts: List[lightdb.Sort]): Option[Bson] = {
    val doc = new BsonDoc()
    sorts.foreach {
      case bf: lightdb.Sort.ByField[_, _] =>
        doc.append(bf.field.name, Integer.valueOf(if bf.direction == SortDirection.Descending then -1 else 1))
      case lightdb.Sort.IndexOrder =>
        // MongoDB natural order isn't a stable insertion order; `_id` ascending is deterministic and,
        // with LightDB's sequential ids, matches insertion order.
        doc.append("_id", Integer.valueOf(1))
      case _ => // BestMatch / ByDistance deferred
    }
    if doc.isEmpty then None else Some(doc)
  }
}
