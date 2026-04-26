package lightdb.tantivy

import fabric.rw.RW
import lightdb.doc.{Document, DocumentModel}
import lightdb.filter.{Condition, Filter}
import scantivy.proto as pb

/** Compiles a `lightdb.filter.Filter[Doc]` into a scantivy `pb.Query`. */
object TantivyFilter {

  def compile[Doc <: Document[Doc]](
    model: DocumentModel[Doc],
    filter: Option[Filter[Doc]]
  ): pb.Query = filter match {
    case None => allQuery
    case Some(f) => compileOne(model, f)
  }

  def allQuery: pb.Query = pb.Query(pb.Query.Node.All(pb.QueryAll()))
  def noneQuery: pb.Query = pb.Query(pb.Query.Node.None(pb.QueryNone()))

  private def compileOne[Doc <: Document[Doc]](
    model: DocumentModel[Doc],
    filter: Filter[Doc]
  ): pb.Query = filter match {
    case Filter.MatchNone() => noneQuery

    case f: Filter.Equals[Doc, _] =>
      val field = model.fieldByName[Any](f.fieldName)
      f.value match {
        case None | null =>
          // `field === None` ↔ "field has no indexed value". Tantivy's RegexQuery `.+` matches
          // any field that has at least one term, so its negation is the "missing field" query.
          pb.Query(pb.Query.Node.BoolQuery(pb.QueryBool(
            must = Seq(allQuery),
            mustNot = Seq(pb.Query(pb.Query.Node.Regex(pb.QueryRegex(field = f.fieldName, pattern = ".+"))))
          )))
        case Some(inner) =>
          singleTerm(model, f.fieldName, inner)
        case it: Iterable[?] if it.isEmpty =>
          noneQuery
        case it: Iterable[?] =>
          // `Set[V] has v` and `List[V] has v` desugar to `Equals(field, Set(v) | List(v))`.
          // For a multi-valued field, "any element equals one of these values".
          val xs = it.toList
          if xs.size == 1 then singleTerm(model, f.fieldName, xs.head)
          else {
            val terms = xs.map(e => TantivyValue.fromAny(field, e))
            pb.Query(pb.Query.Node.InSet(pb.QueryIn(field = f.fieldName, values = terms)))
          }
        case other =>
          singleTerm(model, f.fieldName, other)
      }

    case f: Filter.NotEquals[Doc, _] =>
      val field = model.fieldByName[Any](f.fieldName)
      f.value match {
        case None | null =>
          // `field !== None` ↔ "field has any indexed value".
          pb.Query(pb.Query.Node.Regex(pb.QueryRegex(field = f.fieldName, pattern = ".+")))
        case other =>
          val v = TantivyValue.fromAny(field, other)
          val inner = pb.Query(pb.Query.Node.Term(pb.QueryTerm(field = f.fieldName, value = Some(v))))
          pb.Query(pb.Query.Node.BoolQuery(pb.QueryBool(must = Seq(allQuery), mustNot = Seq(inner))))
      }

    case f: Filter.Regex[Doc, _] =>
      pb.Query(pb.Query.Node.Regex(pb.QueryRegex(field = f.fieldName, pattern = f.expression)))

    case f: Filter.In[Doc, _] =>
      val field = model.fieldByName[Any](f.fieldName)
      val values = f.values.toList.map(v => TantivyValue.fromAny(field, v))
      pb.Query(pb.Query.Node.InSet(pb.QueryIn(field = f.fieldName, values = values)))

    case f: Filter.RangeLong[Doc] =>
      pb.Query(pb.Query.Node.Range(pb.QueryRange(
        field = f.fieldName,
        gte = f.from.map(TantivyValue.long),
        lte = f.to.map(TantivyValue.long)
      )))

    case f: Filter.RangeDouble[Doc] =>
      pb.Query(pb.Query.Node.Range(pb.QueryRange(
        field = f.fieldName,
        gte = f.from.map(TantivyValue.double),
        lte = f.to.map(TantivyValue.double)
      )))

    case f: Filter.StartsWith[Doc, _] =>
      if f.query.isEmpty then allQuery
      else pb.Query(pb.Query.Node.StartsWith(pb.QueryStartsWith(field = f.fieldName, value = f.query)))

    case f: Filter.EndsWith[Doc, _] =>
      if f.query.isEmpty then allQuery
      else pb.Query(pb.Query.Node.EndsWith(pb.QueryEndsWith(field = f.fieldName, value = f.query)))

    case f: Filter.Contains[Doc, _] =>
      if f.query.isEmpty then allQuery
      else pb.Query(pb.Query.Node.Contains(pb.QueryContains(field = f.fieldName, value = f.query)))

    case f: Filter.Exact[Doc, _] =>
      pb.Query(pb.Query.Node.Exact(pb.QueryExact(field = f.fieldName, value = f.query)))

    case m: Filter.Multi[Doc] =>
      val (musts0, shoulds, mustNots) = partition(model, m)
      // Pure must_not (no must / no should) returns nothing in Tantivy; inject a `must=AllQuery`
      // so the negation actually takes effect against the full doc set.
      val musts = if musts0.isEmpty && shoulds.isEmpty && mustNots.nonEmpty then Seq(allQuery) else musts0
      // Only emit minShouldMatch when there are actual should clauses — Tantivy treats
      // `minShouldMatch=1` literally even when shoulds is empty, which makes the query match
      // nothing. LightDB's Filter.Multi defaults `minShould=1` regardless of whether shoulds
      // are present (set by `&&` chaining), so we have to gate on the shoulds list here.
      val minShould =
        if shoulds.nonEmpty && m.minShould > 0 then Some(m.minShould) else None
      val bool = pb.QueryBool(
        must = musts,
        should = shoulds,
        mustNot = mustNots,
        minShouldMatch = minShould
      )
      pb.Query(pb.Query.Node.BoolQuery(bool))

    case d: Filter.DrillDownFacetFilter[Doc] =>
      // Mirror Lucene's $ROOT$ sentinel convention: when the field is hierarchical AND the
      // caller asked for `onlyThisLevel`, the matched docs are those whose facet path ends
      // exactly at this level — i.e. has $ROOT$ appended at this depth.
      val hierarchical = model.fields.collectFirst {
        case ff: lightdb.field.Field.FacetField[Doc] if ff.name == d.fieldName => ff.hierarchical
      }.getOrElse(false)
      val effectivePath: List[String] =
        if hierarchical && d.showOnlyThisLevel then d.path ::: List(TantivyDocConvert.FacetRootSentinel)
        else d.path
      pb.Query(pb.Query.Node.DrillDown(pb.QueryDrillDown(
        field = d.fieldName,
        path = effectivePath,
        onlyThisLevel = d.showOnlyThisLevel
      )))

    case _: Filter.Distance[Doc] | _: Filter.SpatialContains[Doc] | _: Filter.SpatialIntersects[Doc] =>
      throw new UnsupportedOperationException(
        "Tantivy backend does not support spatial filters (no native geo). Use a backend like lucene. " +
          "Spatial is intentionally NOT emulated via in-memory scans because that would mislead callers about real-world performance."
      )

    case _: Filter.Nested[Doc] =>
      // Reachable only if the caller invoked compileOne directly bypassing the doSearch fallback.
      // The `TantivyTransaction` strips nested filters before delegating to compile, so this
      // signals a programming error.
      throw new UnsupportedOperationException(
        "Filter.Nested must be stripped via NestedQuerySupport.stripNested before compilation."
      )

    case _: Filter.ExistsChild[Doc] | _: Filter.ChildConstraints[Doc] =>
      throw new UnsupportedOperationException(
        "Tantivy backend does not support ExistsChild / ChildConstraints filters."
      )

    case other =>
      throw new UnsupportedOperationException(s"Tantivy backend cannot compile filter: $other")
  }

  private def singleTerm[Doc <: Document[Doc]](
    model: DocumentModel[Doc],
    fieldName: String,
    inner: Any
  ): pb.Query = {
    val field = model.fieldByName[Any](fieldName)
    val v = TantivyValue.fromAny(field, inner)
    pb.Query(pb.Query.Node.Term(pb.QueryTerm(field = fieldName, value = Some(v))))
  }

  private def partition[Doc <: Document[Doc]](
    model: DocumentModel[Doc],
    multi: Filter.Multi[Doc]
  ): (Seq[pb.Query], Seq[pb.Query], Seq[pb.Query]) = {
    val musts = scala.collection.mutable.Buffer.empty[pb.Query]
    val shoulds = scala.collection.mutable.Buffer.empty[pb.Query]
    val mustNots = scala.collection.mutable.Buffer.empty[pb.Query]
    multi.filters.foreach { clause =>
      val q = compileOne(model, clause.filter)
      clause.condition match {
        case Condition.Must => musts += q
        case Condition.Filter => musts += q     // filter contributes to match without scoring
        case Condition.MustNot => mustNots += q
        case Condition.Should => shoulds += q
      }
    }
    (musts.toSeq, shoulds.toSeq, mustNots.toSeq)
  }
}
