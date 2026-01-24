package lightdb.traversal.store

import fabric.{Bool, Obj, obj}
import fabric.Str
import fabric.define.DefType
import lightdb.doc.{Document, DocumentModel}
import lightdb.distance.Distance
import lightdb.facet.{FacetQuery, FacetResult, FacetResultValue}
import lightdb.facet.FacetValue
import lightdb.field.Field.FacetField
import lightdb.field.{Field, IndexingState}
import lightdb.filter.{Condition, Filter, FilterClause}
import lightdb.filter.FilterPlanner
import lightdb.id.Id
import lightdb.materialized.{MaterializedAndDoc, MaterializedIndex}
import lightdb.store.Conversion
import lightdb.spatial.{DistanceAndDoc, Geo, Point, Spatial}
import lightdb.{SearchResults, Sort, SortDirection}
import lightdb.KeyValue
import profig.Profig
import fabric.rw.*
import rapid.*

import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Reference traversal-backed query engine.
 *
 * v0 is correctness-first and may use scanning fallback paths.
 * Over time, this engine is intended to be replaced with index-backed planning + execution.
 */
object TraversalQueryEngine {
  // Shared config for bounded seed materialization.
  private lazy val MaxSeedSize: Int = {
    Profig("lightdb.traversal.persistedIndex.maxSeedSize").opt[Int].getOrElse(100_000)
  }

  // Guardrails for expensive verify-only operations (regex/contains) at large scale.
  // Disabled by default for backwards compatibility and test parity.
  private lazy val RequireScopeForVerify: Boolean = {
    Profig("lightdb.traversal.guardrails.verify.requireScope").opt[Boolean].getOrElse(false)
  }
  private lazy val RequireScopeMinCollectionSize: Int =
    Profig("lightdb.traversal.guardrails.verify.minCollectionSize").opt[Int].getOrElse(1_000_000)
  private lazy val RequireScopeAllowEarlyTermination: Boolean =
    Profig("lightdb.traversal.guardrails.verify.allowEarlyTermination").opt[Boolean].getOrElse(true)

  // Shared comparison for sorting + aggregates (null-safe, option-safe).
  private[store] def compareAny(a0: Any, b0: Any): Int = {
    def unwrap(v: Any): Any = v match {
      case null => null
      case Some(x) => unwrap(x)
      case None => null
      case other => other
    }
    val a = unwrap(a0)
    val b = unwrap(b0)
    if a == null && b == null then return 0
    if a == null then return -1
    if b == null then return 1

    (a, b) match {
      case (x: java.lang.Number, y: java.lang.Number) =>
        BigDecimal(x.toString).compare(BigDecimal(y.toString))
      case (x: java.lang.Boolean, y: java.lang.Boolean) =>
        x.compareTo(y)
      case (x: String, y: String) =>
        x.compareTo(y)
      case (x: Comparable[Any @unchecked], y) =>
        try x.compareTo(y)
        catch { case _: Throwable => x.toString.compareTo(y.toString) }
      case _ =>
        a.toString.compareTo(b.toString)
    }
  }
  def search[Doc <: Document[Doc], Model <: DocumentModel[Doc], V](
    storeName: String,
    model: Model,
    backing: lightdb.transaction.PrefixScanningTransaction[Doc, Model],
    indexCache: TraversalIndexCache[Doc, Model],
    persistedIndex: Option[lightdb.transaction.PrefixScanningTransaction[lightdb.KeyValue, lightdb.KeyValue.type]],
    query: lightdb.Query[Doc, Model, V],
  ): Task[SearchResults[Doc, Model, V]] = Task.defer {
    def containsExistsChild(filterOpt: Option[Filter[Doc]]): Boolean = {
      def loop(f: Filter[Doc]): Boolean = f match {
        case _: Filter.ExistsChild[Doc @unchecked] => true
        case m: Filter.Multi[Doc] => m.filters.exists(fc => loop(fc.filter))
        case _ => false
      }
      filterOpt.exists(loop)
    }

    // If ExistsChild was not resolved by the planner (i.e. store supportsNativeExistsChild=true),
    // we do correctness-first fallback resolution here to avoid runtime failures.
    //
    // Special case: for page-only queries with a single ExistsChild, we can avoid materializing all parent ids.
    if containsExistsChild(query.filter.map(_.asInstanceOf[Filter[Doc]])) then {
      // Only safe to do the semi-join early-termination path when we don't need totals/facets and sort is trivial.
      val sorts0 = query.sort
      val limitOpt = query.limit.orElse(query.pageSize)
      val canEarlyTerminate =
        !query.countTotal &&
          query.facets.isEmpty &&
          (sorts0.isEmpty || sorts0 == List(Sort.IndexOrder)) &&
          limitOpt.nonEmpty

      // Try the "single ExistsChild" early-terminating semi-join. Otherwise fallback to planner resolve.
      def stripExistsChild(f: Filter[Doc]): Option[Filter[Doc]] = f match {
        case _: Filter.ExistsChild[Doc @unchecked] => None
        case m: Filter.Multi[Doc] =>
          val clauses = m.filters.filterNot(_.filter.isInstanceOf[Filter.ExistsChild[Doc @unchecked]])
          if clauses.isEmpty then None else Some(m.copy(filters = clauses))
        case other =>
          // Unknown composition; keep as-is (but this means evalFilter would throw). We'll rely on planner fallback.
          Some(other)
      }

      def extractExistsChildren(f: Filter[Doc]): List[Filter.ExistsChild[Doc @unchecked]] = f match {
        case ec: Filter.ExistsChild[Doc @unchecked] => List(ec)
        case m: Filter.Multi[Doc] =>
          m.filters.collect {
            case FilterClause(ec: Filter.ExistsChild[Doc @unchecked], condition, _)
                if (condition == Condition.Must || condition == Condition.Filter) =>
              ec
          }
        case _ => Nil
      }

      def earlyTerminateSemiJoinMulti(
        existsChildren: List[Filter.ExistsChild[Doc @unchecked]],
        originalFilter: Filter[Doc]
      ): Task[SearchResults[Doc, Model, V]] = {
        // verify filter excludes ExistsChild because evalFilter doesn't support it.
        val verifyFilter: Option[Filter[Doc]] = stripExistsChild(originalFilter)
        val offset = query.offset
        val pageSize = limitOpt.get
        val state = new IndexingState
        val needScore: Boolean =
          query.scoreDocs || query.minDocScore.nonEmpty || query.sort.exists(_.isInstanceOf[Sort.BestMatch])

        val driver = existsChildren.head
        val others = existsChildren.tail
        val probeChunkSize: Int = Profig("lightdb.traversal.existsChild.probeChunkSize").opt[Int].getOrElse(128) max 1

        val parentStream: rapid.Stream[(Doc, Double)] =
          rapid.Stream.force {
            driver.relation.childStore.transaction { childTx =>
              val childModel = childTx.store.model
              val driverFilter = driver.childFilter(childModel)
              val parentField = driver.relation.parentField(childModel)

              val seen = mutable.HashSet.empty[String]
              // Fast-path: if the child store is traversal-backed and has a ready persisted index, and the child filter is seedable,
              // stream matching child ids from postings and map childId -> parentId via persisted ref mapping (no child doc loads).
              def seedChildIdsFromPersisted(
                kv: lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type],
                storeName: String
              ): Option[rapid.Stream[String]] = {
                val prefixMaxLen = Profig("lightdb.traversal.persistedIndex.prefixMaxLen").opt[Int].getOrElse(8) max 1
                val maxInTerms = Profig("lightdb.traversal.persistedIndex.streamingSeed.maxInTerms").opt[Int].getOrElse(32) max 1

                def encodedSingleValue(value: Any): Option[String] =
                  TraversalIndex.valuesForIndexValue(value) match {
                    case List(null) => Some("null")
                    case List(s: String) => Some(s.toLowerCase)
                    case List(other) => Some(other.toString)
                    case _ => None
                  }

                driverFilter match {
                  case e: lightdb.filter.Filter.Equals[?, ?] =>
                    val f = childModel.fieldByName[Any](e.fieldName)
                    if !f.indexed then None
                    else if f.isTokenized then {
                      val rawStrOpt: Option[String] = e.value match {
                        case s: String => Some(s)
                        case _ => None
                      }
                      val toks =
                        rawStrOpt.toList
                          .flatMap(_.toLowerCase.split("\\s+").toList.map(_.trim).filter(_.nonEmpty))
                          .distinct
                      toks.headOption.map(tok => TraversalPersistedIndex.postingsStream(TraversalKeys.tokPrefix(storeName, e.fieldName, tok), kv))
                    } else {
                      encodedSingleValue(e.value).map(enc => TraversalPersistedIndex.postingsStream(TraversalKeys.eqPrefix(storeName, e.fieldName, enc), kv))
                    }
                  case s: lightdb.filter.Filter.StartsWith[?, ?] =>
                    val f = childModel.fieldByName[Any](s.fieldName)
                    if !f.indexed then None
                    else Option(s.query).map(_.toLowerCase.take(prefixMaxLen)).filter(_.nonEmpty).map { q =>
                      TraversalPersistedIndex.postingsStream(TraversalKeys.swPrefix(storeName, s.fieldName, q), kv)
                    }
                  case e: lightdb.filter.Filter.EndsWith[?, ?] =>
                    val f = childModel.fieldByName[Any](e.fieldName)
                    if !f.indexed then None
                    else Option(e.query).map(_.toLowerCase.reverse.take(prefixMaxLen)).filter(_.nonEmpty).map { q =>
                      TraversalPersistedIndex.postingsStream(TraversalKeys.ewPrefix(storeName, e.fieldName, q), kv)
                    }
                  case i: lightdb.filter.Filter.In[?, ?] =>
                    val f = childModel.fieldByName[Any](i.fieldName)
                    val values0 = i.values.asInstanceOf[Seq[Any]].take(maxInTerms).toList
                    val encOpt = values0.map(encodedSingleValue)
                    if !f.indexed || values0.isEmpty || encOpt.exists(_.isEmpty) then None
                    else {
                      val prefixes = encOpt.flatten.distinct.map(enc => TraversalKeys.eqPrefix(storeName, i.fieldName, enc))
                      val seenIds = mutable.HashSet.empty[String]
                      Some(
                        rapid.Stream.merge {
                          Task.pure {
                            val it = prefixes.iterator.map(pfx => TraversalPersistedIndex.postingsStream(pfx, kv))
                            Pull.fromIterator(it)
                          }
                        }.collect { case id if seenIds.add(id) => id }
                      )
                    }
                  case _ =>
                    None
                }
              }

              val parentIds: rapid.Stream[Id[Doc]] = childTx match {
                case ttx: TraversalTransaction[?, ?] if ttx.store.persistedIndexEnabled && ttx.store.name != "_backingStore" =>
                  ttx.store.effectiveIndexBacking match {
                    case None =>
                      // fallback to scan path
                      childTx.query.filter(_ => driverFilter).value(_ => parentField).stream
                    case Some(idx) =>
                      rapid.Stream.force {
                        idx.transaction { kv0 =>
                          val kv = kv0.asInstanceOf[lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type]]
                          TraversalPersistedIndex.isReady(ttx.store.name, kv).map { ready =>
                            if !ready then {
                              childTx.query.filter(_ => driverFilter).value(_ => parentField).stream
                            } else {
                              seedChildIdsFromPersisted(kv, ttx.store.name) match {
                                case Some(childIds) =>
                                  childIds
                                    .evalMap(cid => TraversalPersistedIndex.refGet(ttx.store.name, parentField.name, cid, kv))
                                    .collect { case Some(pid) => Id[Doc](pid) }
                                case None =>
                                  childTx.query.filter(_ => driverFilter).value(_ => parentField).stream
                              }
                            }
                          }
                        }
                      }
                  }
                case _ =>
                  childTx.query.filter(_ => driverFilter).value(_ => parentField).stream
              }

              val s =
                parentIds
                  .collect { case pid if pid != null && seen.add(pid.value) => pid }
                  .chunk(probeChunkSize)
                  .evalMap { idsChunk =>
                    val base: Set[Id[Doc]] = idsChunk.toList.toSet
                    if others.isEmpty then Task.pure(base.toList)
                    else {
                      // Intersect each clause's parent-id hits against the current candidate chunk.
                      others.foldLeft(Task.pure(base)) { (accT, ec) =>
                        accT.flatMap { acc =>
                          if acc.isEmpty then Task.pure(Set.empty)
                          else {
                            ec.relation.childStore.transaction { tx2 =>
                              val m2 = tx2.store.model
                              val cf2 = ec.childFilter(m2)
                              val pf2 = ec.relation.parentField(m2)
                              tx2.query
                                .filter(_ => cf2)
                                .filter(_ => Filter.In(pf2.name, acc.toSeq))
                                .value(_ => pf2)
                                .toList
                                .map(_.toSet)
                            }.map(hits => acc intersect hits)
                          }
                        }
                      }.map(_.toList)
                    }
                  }
                  .flatMap(list => rapid.Stream.emits(list))
                  .evalMap(pid => backing.get(pid))
                  .collect { case Some(doc) => doc }
                  .evalMap { doc =>
                    Task {
                      val passesOther = verifyFilter.forall(f => evalFilter(f, model, doc, state))
                      if !passesOther then None
                      else {
                        val score = if needScore then bestMatchScore(verifyFilter, model, doc, state) else 0.0
                        val passesScore = query.minDocScore.forall(min => score >= min)
                        if passesScore then Some(doc -> score) else None
                      }
                    }
                  }
                  .collect { case Some(v) => v }
              Task.pure(s)
            }
          }

        val paged = parentStream.drop(offset).take(pageSize)
        val streamWithScore = paged.map { case (doc, score) =>
          convert(doc, query.conversion, model, state, Map.empty) -> score
        }

        Task.pure(
          SearchResults(
            model = model,
            offset = offset,
            limit = limitOpt,
            total = None,
            streamWithScore = streamWithScore,
            facetResults = Map.empty,
            transaction = query.transaction
          )
        )
      }

      val plannerFallback: Task[SearchResults[Doc, Model, V]] =
        FilterPlanner
          .resolve(query.filter.map(_.asInstanceOf[Filter[Doc]]), model, resolveExistsChild = true)
          .flatMap(resolved => search(storeName, model, backing, indexCache, persistedIndex, query.copy(filter = resolved)))

      val earlyTaskOpt: Option[Task[SearchResults[Doc, Model, V]]] =
        if !canEarlyTerminate then None
        else {
          query.filter.flatMap { f0 =>
            f0.asInstanceOf[Filter[Doc]] match {
              case m: Filter.Multi[Doc] =>
                val ecs = extractExistsChildren(m)
                if ecs.isEmpty then None else Some(earlyTerminateSemiJoinMulti(ecs, f0.asInstanceOf[Filter[Doc]]))
              case ec: Filter.ExistsChild[Doc @unchecked] =>
                Some(earlyTerminateSemiJoinMulti(List(ec), f0.asInstanceOf[Filter[Doc]]))
              case _ => None
            }
          }
        }

      // Parent-driven page-only semi-join (opt-in): when parent-side has a small seed, probe children by parent id
      // instead of scanning broad child filters to discover parent ids.
      def boolProfig(key: String, default: Boolean): Boolean = {
        Profig(key).get() match {
          case Some(Bool(b, _)) => b
          case Some(o: Obj) =>
            o.get("enabled") match {
              case Some(Bool(b, _)) => b
              case Some(other) => other.asBoolean
              case None => true
            }
          case _ =>
            default
        }
      }

      def intProfig(key: String, default: Int): Int =
        Profig(key).opt[Int].getOrElse(default)

      val parentDrivenEnabled: Boolean =
        boolProfig("lightdb.traversal.existsChild.parentDriven", default = false)
      val parentDrivenMaxParents: Int =
        intProfig("lightdb.traversal.existsChild.parentDriven.maxParents", default = 1024) max 1

      val parentDrivenTaskOpt: Option[Task[SearchResults[Doc, Model, V]]] =
        if !canEarlyTerminate || !parentDrivenEnabled then None
        else {
          (persistedIndex, query.filter) match {
            case (Some(parentKv), Some(f0)) =>
              val original = f0.asInstanceOf[Filter[Doc]]
              val existsChildren = extractExistsChildren(original)
              if existsChildren.isEmpty then None
              else {
                val verifyFilter = stripExistsChild(original)

                Some(
                  TraversalPersistedIndex.isReady(storeName, parentKv).flatMap { ready =>
                    if !ready then plannerFallback
                    else {
                      seedCandidatesPersisted(storeName, model, verifyFilter, parentKv).flatMap {
                        case Some(ids) if ids.nonEmpty && ids.size <= parentDrivenMaxParents =>
                          val orderedParentIds = ids.toList.sorted
                          val offset = query.offset
                          val pageSize = limitOpt.get
                          val need = offset + pageSize
                          val state = new IndexingState

                          def childHasMatch(ec: Filter.ExistsChild[Doc @unchecked], pid: Id[Doc]): Task[Boolean] =
                            ec.relation.childStore.transaction { childTx =>
                              val childModel = childTx.store.model
                              val cf = ec.childFilter(childModel)
                              val parentField = ec.relation.parentField(childModel)

                              // Probe children for this parent; execution will seed by parentId equality postings when available.
                              childTx.query
                                .clearPageSize
                                .limit(1)
                                .filter(_ => cf)
                                .filter(_ => Filter.Equals(parentField.name, pid))
                                .id
                                .stream
                                .take(1)
                                .toList
                                .map(_.nonEmpty)
                            }

                          val parentStream: rapid.Stream[(Doc, Double)] =
                            rapid.Stream
                              .emits(orderedParentIds)
                              .map(id => Id[Doc](id))
                              .evalMap(id => backing.get(id))
                              .collect { case Some(doc) => doc }
                              .evalMap { doc =>
                                val pid = doc._id
                                val parentOk = verifyFilter.forall(vf => evalFilter(vf, model, doc, state))
                                if !parentOk then Task.pure(None)
                                else {
                                  existsChildren
                                    .foldLeft(Task.pure(true)) { (accT, ec) =>
                                      accT.flatMap {
                                        case false => Task.pure(false)
                                        case true => childHasMatch(ec, pid)
                                      }
                                    }
                                    .map {
                                      case true => Some(doc -> 0.0)
                                      case false => None
                                    }
                                }
                              }
                              .collect { case Some(v) => v }

                          val paged = parentStream.take(need).drop(offset).take(pageSize)
                          Task.pure(
                            SearchResults(
                              model = model,
                              offset = offset,
                              limit = limitOpt,
                              total = None,
                              streamWithScore = paged.map { case (doc, score) =>
                                convert(doc, query.conversion, model, state, Map.empty) -> score
                              },
                              facetResults = Map.empty,
                              transaction = query.transaction
                            )
                          )
                        case _ =>
                          plannerFallback
                      }
                    }
                  }
                )
              }
            case _ =>
              None
          }
        }

      // Optional "native full" ExistsChild resolution (not page-only): resolve to a bounded parent-id IN filter
      // without going through FilterPlanner/ExistsChild.resolve (which may materialize huge sets or be capped too low).
      //
      // This stays disabled by default; enable with:
      // -Dlightdb.traversal.existsChild.nativeFull=true
      val nativeFullEnabled: Boolean =
        boolProfig("lightdb.traversal.existsChild.nativeFull", default = false)
      val nativeFullMaxParents: Int =
        intProfig("lightdb.traversal.existsChild.nativeFull.maxParentIds", default = 100_000) max 0

      val nativeFullTaskOpt: Option[Task[SearchResults[Doc, Model, V]]] =
        if !nativeFullEnabled then None
        else {
          query.filter.flatMap { f0 =>
            val original = f0.asInstanceOf[Filter[Doc]]
            val existsChildren: List[Filter.ExistsChild[Doc @unchecked]] = original match {
              case m: Filter.Multi[Doc] => extractExistsChildren(m)
              case ec: Filter.ExistsChild[Doc @unchecked] => List(ec)
              case _ => Nil
            }
            if existsChildren.isEmpty || nativeFullMaxParents <= 0 then None
            else {
              val verifyFilter: Option[Filter[Doc]] = stripExistsChild(original)

              def parentIdsFor(ec: Filter.ExistsChild[Doc @unchecked]): Task[Option[Set[String]]] =
                ec.relation.childStore.transaction { childTx =>
                  val childModel = childTx.store.model
                  val cf = ec.childFilter(childModel)
                  val parentField = ec.relation.parentField(childModel)

                  final class TooManyParents extends RuntimeException("too many parents")
                  def fallbackScan(): Task[Option[Set[String]]] = {
                    val seen = mutable.HashSet.empty[String]
                    childTx.query
                      .filter(_ => cf)
                      .value(_ => parentField)
                      .stream
                      .evalMap { (pid: Id[Doc]) =>
                        Task {
                          if pid != null then {
                            seen.add(pid.value)
                            if seen.size > nativeFullMaxParents then throw new TooManyParents
                          }
                        }
                      }
                      .drain
                      .attempt
                      .map {
                        case scala.util.Success(_) => Some(seen.toSet)
                        case scala.util.Failure(_: TooManyParents) => None
                        case scala.util.Failure(t) => throw t
                      }
                  }

                  // Fast path: if the child store is traversal-backed and has a ready persisted index, map childId -> parentId
                  // via the per-doc ref mapping (ti:<childStore>:ref:<parentField>:<childId> -> "<parentId>") without loading docs.
                  childTx match {
                    case ttx: TraversalTransaction[?, ?] if ttx.store.persistedIndexEnabled && ttx.store.name != "_backingStore" =>
                      ttx.store.effectiveIndexBacking match {
                        case None =>
                          fallbackScan()
                        case Some(idx) =>
                          idx
                            .transaction { kv0 =>
                            val kv = kv0.asInstanceOf[lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type]]
                            TraversalPersistedIndex.isReady(ttx.store.name, kv).flatMap { ready =>
                              if !ready then fallbackScan()
                              else {
                                val seen = mutable.HashSet.empty[String]
                                // If the child filter is seedable from persisted postings, stream child ids from postings
                                // (avoids scanning + doc loads). Otherwise fall back to child query scan.
                                val prefixMaxLen = Profig("lightdb.traversal.persistedIndex.prefixMaxLen").opt[Int].getOrElse(8) max 1
                                val maxInTerms = Profig("lightdb.traversal.persistedIndex.streamingSeed.maxInTerms").opt[Int].getOrElse(32) max 1

                                def encodedSingleValue(value: Any): Option[String] =
                                  TraversalIndex.valuesForIndexValue(value) match {
                                    case List(null) => Some("null")
                                    case List(s: String) => Some(s.toLowerCase)
                                    case List(other) => Some(other.toString)
                                    case _ => None
                                  }

                                def seedChildIdsFromPersisted: Option[rapid.Stream[String]] =
                                  cf match {
                                    case e: lightdb.filter.Filter.Equals[?, ?] =>
                                      val f = childModel.fieldByName[Any](e.fieldName)
                                      if !f.indexed then None
                                      else if f.isTokenized then {
                                        val rawStrOpt: Option[String] = e.value match {
                                          case s: String => Some(s)
                                          case _ => None
                                        }
                                        val toks =
                                          rawStrOpt.toList
                                            .flatMap(_.toLowerCase.split("\\s+").toList.map(_.trim).filter(_.nonEmpty))
                                            .distinct
                                        toks.headOption.map { tok =>
                                          TraversalPersistedIndex.postingsStream(TraversalKeys.tokPrefix(ttx.store.name, e.fieldName, tok), kv)
                                        }
                                      } else {
                                        encodedSingleValue(e.value).map { enc =>
                                          TraversalPersistedIndex.postingsStream(TraversalKeys.eqPrefix(ttx.store.name, e.fieldName, enc), kv)
                                        }
                                      }
                                    case s: lightdb.filter.Filter.StartsWith[?, ?] =>
                                      val f = childModel.fieldByName[Any](s.fieldName)
                                      if !f.indexed then None
                                      else {
                                        Option(s.query).map(_.toLowerCase.take(prefixMaxLen)).filter(_.nonEmpty).map { q =>
                                          TraversalPersistedIndex.postingsStream(TraversalKeys.swPrefix(ttx.store.name, s.fieldName, q), kv)
                                        }
                                      }
                                    case e: lightdb.filter.Filter.EndsWith[?, ?] =>
                                      val f = childModel.fieldByName[Any](e.fieldName)
                                      if !f.indexed then None
                                      else {
                                        Option(e.query).map(_.toLowerCase.reverse.take(prefixMaxLen)).filter(_.nonEmpty).map { q =>
                                          TraversalPersistedIndex.postingsStream(TraversalKeys.ewPrefix(ttx.store.name, e.fieldName, q), kv)
                                        }
                                      }
                                    case i: lightdb.filter.Filter.In[?, ?] =>
                                      val f = childModel.fieldByName[Any](i.fieldName)
                                      val values0 = i.values.asInstanceOf[Seq[Any]].take(maxInTerms).toList
                                      val encOpt = values0.map(encodedSingleValue)
                                      if !f.indexed || values0.isEmpty || encOpt.exists(_.isEmpty) then None
                                      else {
                                        val prefixes = encOpt.flatten.distinct.map(enc => TraversalKeys.eqPrefix(ttx.store.name, i.fieldName, enc))
                                        val seenIds = mutable.HashSet.empty[String]
                                        Some(
                                          rapid.Stream.merge {
                                            Task.pure {
                                              val it = prefixes.iterator.map(pfx => TraversalPersistedIndex.postingsStream(pfx, kv))
                                              Pull.fromIterator(it)
                                            }
                                          }.collect { case id if seenIds.add(id) => id }
                                        )
                                      }
                                    case _ =>
                                      None
                                  }

                                val childIds: rapid.Stream[String] =
                                  seedChildIdsFromPersisted.getOrElse {
                                    // fallback: scan via child query (may still seed internally, but will load docs)
                                    childTx.query.filter(_ => cf).id.stream.map(_.value)
                                  }

                                childIds
                                  .evalMap { cidStr =>
                                    TraversalPersistedIndex.refGet(ttx.store.name, parentField.name, cidStr, kv).map {
                                      case Some(pid) =>
                                        seen.add(pid)
                                        if seen.size > nativeFullMaxParents then throw new TooManyParents
                                        true
                                      case None =>
                                        throw new RuntimeException("missing child->parent ref mapping")
                                    }
                                  }
                                  .drain
                                  .attempt
                                  .map {
                                    case scala.util.Success(_) => Some(seen.toSet)
                                    case scala.util.Failure(_: TooManyParents) => None
                                    case scala.util.Failure(t) => throw t
                                  }
                              }
                            }
                          }
                            .attempt
                            .flatMap {
                              case scala.util.Success(opt) => Task.pure(opt)
                              case scala.util.Failure(_) => fallbackScan()
                            }
                      }
                    case _ =>
                      fallbackScan()
                  }
                }

              Some(
                existsChildren.map(parentIdsFor).tasks.flatMap { setsOpt =>
                  if setsOpt.exists(_.isEmpty) then plannerFallback
                  else {
                    val sets = setsOpt.flatten
                    if sets.isEmpty then plannerFallback
                    else {
                      val base = sets.reduceLeft(_ intersect _)
                      val inFilter =
                        if base.isEmpty then Filter.MatchNone[Doc]()
                        else Filter.In[Doc, Id[Doc]](model._id.name, base.toSeq.map(id => Id[Doc](id)))
                      val rewritten: Option[Filter[Doc]] = verifyFilter match {
                        case None => Some(inFilter)
                        case Some(vf) =>
                          Some(Filter.Multi[Doc](0, List(
                            FilterClause(inFilter, Condition.Must, None),
                            FilterClause(vf, Condition.Must, None)
                          )))
                      }
                      search(storeName, model, backing, indexCache, persistedIndex, query.copy(filter = rewritten))
                    }
                  }
                }
              )
            }
          }
        }

      parentDrivenTaskOpt.orElse(earlyTaskOpt).orElse(nativeFullTaskOpt).getOrElse(plannerFallback)
    } else {

    val state = new IndexingState
    val needScore: Boolean =
      query.scoreDocs || query.minDocScore.nonEmpty || query.sort.exists(_.isInstanceOf[Sort.BestMatch])
    val sorts = query.sort

    // NOTE: rapid.Stream doesn't expose an Iterator without materializing, so we consume the stream
    // and accumulate only what we need (page + bounded sort heap), while still scanning for totals/facets.
    final case class Hit(doc: Doc, score: Double, keys: List[Any])

    def dirFor(sort: Sort): SortDirection = sort match {
      case Sort.BestMatch(direction) => direction
      case Sort.ByField(_, direction) => direction
      case Sort.IndexOrder => SortDirection.Ascending
      case Sort.ByDistance(_, _, direction) => direction
    }

    def distanceMin(doc: Doc, field: Field[Doc, List[Geo]], from: Point): Double = {
      // Some backends historically returned unexpected collection shapes; widen to Any so Scala 2 doesn't reject patterns.
      val raw: Any = field.get(doc, field, state)
      val list = raw match {
        case null => Nil
        case l: List[_] => l.collect { case g: Geo => g }
        case s: Set[_] => s.toList.collect { case g: Geo => g }
        case g: Geo => List(g)
        case other => List(other).collect { case g: Geo => g }
      }
      list.map(g => Spatial.distance(g, from).valueInMeters).minOption.getOrElse(Double.PositiveInfinity)
    }

    def keysFor(doc: Doc, score: Double): List[Any] =
      sorts.map {
        case Sort.BestMatch(_) => score
        case Sort.IndexOrder => doc._id.value
        case Sort.ByField(field, _) =>
          val f = field.asInstanceOf[Field[Doc, Any]]
          f.get(doc, f, state)
        case Sort.ByDistance(field, from, _) =>
          distanceMin(doc, field.asInstanceOf[Field[Doc, List[Geo]]], from)
      }

    def compareHits(a: Hit, b: Hit): Int = {
      val cmp = sorts.iterator.zip(a.keys.iterator.zip(b.keys.iterator)).map { case (sort, (k1, k2)) =>
        val base = compareAny(k1, k2)
        if dirFor(sort) == SortDirection.Descending then -base else base
      }.find(_ != 0).getOrElse(0)

      if cmp != 0 then cmp
      else a.doc._id.value.compareTo(b.doc._id.value)
    }

    def matches(doc: Doc): Boolean = query.filter match {
      case None => true
      case Some(f) => evalFilter(f.asInstanceOf[Filter[Doc]], model, doc, state)
    }

    // Candidate seeding (in-memory cache OR persisted postings).
    val seedIds: Option[Set[Id[Doc]]] = {
      val seededByCache =
        if indexCache.enabled then
          seedCandidates(storeName, model, query.filter, indexCache).map(_.map(id => Id[Doc](id)).toSet)
        else None

      seededByCache.orElse {
        // persisted index seeding is computed asynchronously below
        None
      }
    }

    val seedIdsTask: Task[Option[Set[Id[Doc]]]] = seedIds match {
      case some @ Some(_) => Task.pure(some)
      case None =>
        persistedIndex match {
          case Some(kv) =>
            TraversalPersistedIndex.isReady(storeName, kv).flatMap { ready =>
              if !ready then Task.pure(None)
              else seedCandidatesPersisted(storeName, model, query.filter, kv).map(_.map(_.map(id => Id[Doc](id)).toSet))
            }
          case None =>
            Task.pure(None)
        }
    }

    seedIdsTask.flatMap { seedIdsResolved =>
      // Optional guardrails: avoid accidental full scans with verify-only filters (regex/contains) at massive scale.
      def hasUnscopedVerifyFilter(filter: Option[Filter[Doc]]): Boolean = {
        def loop(f: Filter[Doc]): Boolean = f match {
          case c: Filter.Contains[Doc, _] =>
            // N-gram seeding only works reliably for >= 3 chars; shorter contains implies verify-only scan.
            val q = Option(c.query).getOrElse("")
            q.nonEmpty && q.length < 3
          case r: Filter.Regex[Doc, _] =>
            extractLiteralFromRegex(r.expression) match {
              case Some(lit) => lit.length < 3
              case None => true
            }
          case m: Filter.Multi[Doc] =>
            m.filters.exists(fc => loop(fc.filter))
          case _ =>
            false
        }
        filter.exists(loop)
      }

      val offset = query.offset
      val limit = query.limit.orElse(query.pageSize)
      val limitIntOpt = limit.map(_.toInt)

      def isExactSeedable(f: Filter[Doc]): Boolean = f match {
        case _: Filter.MatchNone[Doc] => true
        case e: Filter.Equals[Doc, _] => e.field(model).indexed
        case i: Filter.In[Doc, _] => i.field(model).indexed
        case r: Filter.RangeLong[Doc] => r.field(model).indexed
        case r: Filter.RangeDouble[Doc] => r.field(model).indexed
        case _: Filter.NotEquals[Doc, _] => true // only meaningful when combined with an exact base seed
        case m: Filter.Multi[Doc] =>
          // Conservative: only MUST/FILTER/MUST_NOT with no SHOULD thresholding.
          val hasShould = m.filters.exists(_.condition == Condition.Should)
          if hasShould && m.minShould > 0 then false
          else m.filters.forall(fc => isExactSeedable(fc.filter))
        case _ => false
      }

      // Fast total when query has no filter: avoid scanning just to count.
      // This is a big win for Query.streamScored which needs totals to page through results.
      val fastTotalTask: Task[Option[Int]] =
        if !query.countTotal then Task.pure(None)
        else {
          val prefixMaxLenForTotal: Int =
            Profig("lightdb.traversal.persistedIndex.prefixMaxLen").opt[Int].getOrElse(8) max 1
          val prefixFastTotalEnabled: Boolean =
            Profig("lightdb.traversal.persistedIndex.fastTotal.prefix.enabled").opt[Boolean].getOrElse(false)
          (query.filter, seedIdsResolved) match {
            case (None, None) =>
              backing.count.map(Some(_))
            case (Some(f0), Some(ids)) if ids.nonEmpty && isExactSeedable(f0.asInstanceOf[Filter[Doc]]) =>
              Task.pure(Some(ids.size))
            case (Some(f0), None) =>
              // If the persisted index is ready but the seed is too large to materialize (maxSeedSize),
              // we can still compute an exact total by counting postings for the simplest exact case (Equals).
              (persistedIndex, f0.asInstanceOf[Filter[Doc]]) match {
                case (Some(kv), e: Filter.Equals[Doc, _]) if e.field(model).indexed =>
                  TraversalPersistedIndex.isReady(storeName, kv).flatMap { ready =>
                    if !ready then Task.pure(None)
                    else {
                      if e.field(model).isTokenized then {
                        e.getJson(model) match {
                          case fabric.Str(s, _) =>
                            TraversalPersistedIndex
                              .tokPostingsCountIfSingleToken(storeName, e.fieldName, s, kv)
                              .flatMap {
                                case some @ Some(_) => Task.pure(some)
                                case None =>
                                  // Best-effort: for small token queries, compute exact intersection count from token postings.
                                  TraversalPersistedIndex.tokPostingsCountIfSmallTokens(storeName, e.fieldName, s, kv)
                              }
                          case _ =>
                            Task.pure(None)
                        }
                      } else {
                        TraversalPersistedIndex.eqPostingsCountIfSingleValue(storeName, e.fieldName, e.value, kv)
                      }
                    }
                  }
                case (Some(kv), s: Filter.StartsWith[Doc, _]) if s.field(model).indexed =>
                  TraversalPersistedIndex.isReady(storeName, kv).flatMap { ready =>
                    val q = Option(s.query).getOrElse("")
                    // Exact only when postings are not truncated AND we avoid case-fold overcount.
                    //
                    // NOTE: postings for string values are lowercased, but Filter.StartsWith is case-sensitive. A postings-count
                    // is only exact if the application normalizes stored values (and queries) consistently. Keep this opt-in.
                    val exact =
                      prefixFastTotalEnabled &&
                        q.nonEmpty &&
                        q.length <= prefixMaxLenForTotal &&
                        q == q.toLowerCase
                    if !ready || !exact then Task.pure(None)
                    else {
                      val pfx = TraversalKeys.swPrefix(storeName, s.fieldName, q)
                      TraversalPersistedIndex.postingsCount(pfx, kv).map(Some(_))
                    }
                  }
                case (Some(kv), e: Filter.EndsWith[Doc, _]) if e.field(model).indexed =>
                  TraversalPersistedIndex.isReady(storeName, kv).flatMap { ready =>
                    val q = Option(e.query).getOrElse("")
                    val exact =
                      prefixFastTotalEnabled &&
                        q.nonEmpty &&
                        q.length <= prefixMaxLenForTotal &&
                        q == q.toLowerCase
                    if !ready || !exact then Task.pure(None)
                    else {
                      val pfx = TraversalKeys.ewPrefix(storeName, e.fieldName, q.reverse)
                      TraversalPersistedIndex.postingsCount(pfx, kv).map(Some(_))
                    }
                  }
                case (Some(kv), in: Filter.In[Doc, _]) if in.field(model).indexed =>
                  TraversalPersistedIndex.isReady(storeName, kv).flatMap { ready =>
                    // Exact only when field is single-valued (non-array) so term postings are disjoint, and when values are non-string.
                    val field = in.field(model).asInstanceOf[Field[Doc, Any]]
                    val maxTerms = Profig("lightdb.traversal.persistedIndex.fastTotal.in.maxTerms").opt[Int].getOrElse(32) max 1
                    val values0 = in.values.asInstanceOf[Seq[Any]].toList.distinct.take(maxTerms + 1)
                    val safe =
                      ready &&
                        !field.isArr &&
                        values0.nonEmpty &&
                        values0.size <= maxTerms &&
                        values0.forall(v => !TraversalIndex.valuesForIndexValue(v).exists(_.isInstanceOf[String])) &&
                        values0.forall(v => TraversalIndex.valuesForIndexValue(v).size == 1)

                    if !safe then Task.pure(None)
                    else {
                      val encoded: List[String] = values0.map { v =>
                        TraversalIndex.valuesForIndexValue(v).headOption match {
                          case None => "null"
                          case Some(null) => "null"
                          case Some(s: String) => s.toLowerCase
                          case Some(o) => o.toString
                        }
                      }
                      encoded
                        .map(v => TraversalPersistedIndex.postingsCount(TraversalKeys.eqPrefix(storeName, in.fieldName, v), kv))
                        .tasks
                        .map(cs => Some(cs.sum))
                    }
                  }
                case _ =>
                  Task.pure(None)
              }
            case _ =>
              Task.pure(None)
          }
        }
      val canEarlyTerminateForGuardrail: Boolean =
        !query.countTotal &&
          query.facets.isEmpty &&
          (sorts.isEmpty || sorts == List(Sort.IndexOrder)) &&
          limitIntOpt.nonEmpty

      val guardrailNeeded =
        RequireScopeForVerify &&
          seedIdsResolved.isEmpty &&
          hasUnscopedVerifyFilter(query.filter) &&
          (!canEarlyTerminateForGuardrail || !RequireScopeAllowEarlyTermination)

      val guardrailTask: Task[Unit] =
        if !guardrailNeeded then Task.unit
        else {
          backing.count.flatMap { c =>
            if c >= RequireScopeMinCollectionSize then {
              Task {
                throw new IllegalArgumentException(
                  s"Traversal guardrail: query contains unscoped verify-only regex/contains over large collection ($c docs). " +
                    s"Add a selective indexed scope (Equals/In/StartsWith/Range/etc), use a seedable literal of length >= 3, " +
                    s"or disable with 'lightdb.traversal.guardrails.verify.requireScope=false'."
                )
              }
            } else Task.unit
          }
        }

      guardrailTask.next {
        fastTotalTask.flatMap { fastTotal =>
        // Streaming candidate seed (persisted index) for page-only queries when seed materialization is skipped.
        // NOTE: ordering will be "posting key order", NOT doc index order. We only use this when query has no explicit sort.
        val streamingSeedEnabled: Boolean =
          Profig("lightdb.traversal.persistedIndex.streamingSeed.enabled").opt[Boolean].getOrElse(true)
        val streamingPrefixMaxLen: Int =
          Profig("lightdb.traversal.persistedIndex.prefixMaxLen").opt[Int].getOrElse(8) max 1
        val streamingMaxInTerms: Int =
          Profig("lightdb.traversal.persistedIndex.streamingSeed.maxInTerms").opt[Int].getOrElse(32) max 1
        val streamingMaxInPageDocs: Int =
          Profig("lightdb.traversal.persistedIndex.streamingSeed.maxInPageDocs").opt[Int].getOrElse(10_000) max 1
        val streamingMaxRangePrefixes: Int =
          Profig("lightdb.traversal.persistedIndex.streamingSeed.maxRangePrefixes").opt[Int].getOrElse(256) max 1
        val streamingMaxRangePageDocs: Int =
          Profig("lightdb.traversal.persistedIndex.streamingSeed.maxRangePageDocs").opt[Int].getOrElse(10_000) max 1
        val streamingIndexOrderRangeOversample: Int =
          Profig("lightdb.traversal.persistedIndex.streamingSeed.indexOrder.rangeOversample").opt[Int].getOrElse(10) max 1
        val streamingIndexOrderPrefixOversample: Int =
          Profig("lightdb.traversal.persistedIndex.streamingSeed.indexOrder.prefixOversample").opt[Int].getOrElse(4) max 1
        val streamingMultiDriverMaxCount: Int =
          Profig("lightdb.traversal.persistedIndex.streamingSeed.multi.driverMaxCount").opt[Int].getOrElse(100_000) max 1
        val streamingMultiDriverMaxCountPrefixes: Int =
          Profig("lightdb.traversal.persistedIndex.streamingSeed.multi.driverMaxCountPrefixes").opt[Int].getOrElse(64) max 1
        val streamingMultiMaxShouldDrivers: Int =
          Profig("lightdb.traversal.persistedIndex.streamingSeed.multi.maxShouldDrivers").opt[Int].getOrElse(8) max 1
        val streamingSortByFieldEnabled: Boolean =
          Profig("lightdb.traversal.streamingSortByField.enabled").opt[Boolean].getOrElse(false)
        val orderByFieldPostingsEnabled: Boolean =
          Profig("lightdb.traversal.orderByFieldPostings.enabled").opt[Boolean].getOrElse(false)

        def streamedEqualsDocs(kv: lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type],
                               e: Filter.Equals[Doc, _]): rapid.Stream[Doc] = {
          val rawValues = TraversalIndex.valuesForIndexValue(e.value)
          val encoded = rawValues.headOption.map {
            case null => "null"
            case s: String => s.toLowerCase
            case v => v.toString
          }.getOrElse("")
          val prefix = TraversalKeys.eqoPrefix(storeName, e.fieldName, encoded)
          TraversalPersistedIndex.postingsStream(prefix, kv)
            .map(s => Id[Doc](s))
            .evalMap(id => backing.get(id))
            .collect { case Some(doc) => doc }
        }

        def streamedTokenizedEqualsDocs(
          kv: lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type],
          e: Filter.Equals[Doc, _]
        ): Option[rapid.Stream[Doc]] = {
          val field = e.field(model)
          if !field.isTokenized then None
          else {
            // Tokenized Equals means "all tokens present" (AND). Persisted postings are per token, so we seed by a single
            // token postings stream and verify the full filter later.
            val raw: Option[String] =
              e.getJson(model) match {
                case fabric.Str(s, _) => Some(s)
                case _ => None
              }
            val tokens =
              raw.toList
                .flatMap(_.toLowerCase.split("\\s+").toList.map(_.trim).filter(_.nonEmpty))
                .distinct
            if tokens.isEmpty then None
            else {
              // Choose the smallest token postings as the seed driver (bounded), to reduce verify work.
              // For IndexOrder + page-only paths, avoid double-scanning by doing take+count in one pass per token prefix.
              val isIndexOrder = query.sort == List(Sort.IndexOrder)
              val takeN0 =
                if limitIntOpt.isEmpty then Int.MaxValue
                else (offset + limitIntOpt.get * streamingIndexOrderPrefixOversample) max 0

              if !isIndexOrder || takeN0 == Int.MaxValue then {
                // Non-IndexOrder paths can stream an entire postings list, but we still avoid double scans by taking a small
                // sample + count hint in one pass per token prefix, then streaming that prefix fully.
                val prefixes = tokens.map(tok => TraversalKeys.tokPrefix(storeName, e.fieldName, tok))
                val chosenT: Task[String] =
                  prefixes
                    .map(pfx => TraversalPersistedIndex.postingsTakeAndCountUpTo(pfx, kv, 1, streamingMultiDriverMaxCount).map { case (_, c) => pfx -> c })
                    .tasks
                    .map(_.minBy(_._2)._1)
                Some(
                  rapid.Stream.force(
                    chosenT.map { bestPrefix =>
                      TraversalPersistedIndex
                        .postingsStream(bestPrefix, kv)
                        .map(docId => Id[Doc](docId))
                        .evalMap(id => backing.get(id))
                        .collect { case Some(doc) => doc }
                    }
                  )
                )
              } else {
                val prefixes = tokens.map(tok => TraversalKeys.tokoPrefix(storeName, e.fieldName, tok))
                val takenT: Task[List[(List[String], Int)]] =
                  prefixes
                    .map(pfx => TraversalPersistedIndex.postingsTakeAndCountUpTo(pfx, kv, takeN0, streamingMultiDriverMaxCount))
                    .tasks

                Some(
                  rapid.Stream.force(
                    takenT.map { taken =>
                      // Choose prefix with smallest count hint; fall back to smallest taken list size.
                      val best = taken.minBy { case (ids, c) => (c, ids.size) }._1
                      rapid.Stream
                        .emits(best)
                        .map(docId => Id[Doc](docId))
                        .evalMap(id => backing.get(id))
                        .collect { case Some(doc) => doc }
                    }
                  )
                )
              }
            }
          }
        }

        def streamedStartsWithDocs(
          kv: lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type],
          s: Filter.StartsWith[Doc, _]
        ): rapid.Stream[Doc] = {
          val q = Option(s.query).getOrElse("")
          if q.isEmpty then rapid.Stream.empty
          else {
            val p = q.toLowerCase.take(streamingPrefixMaxLen)
            val prefix =
              if query.sort == List(Sort.IndexOrder) then TraversalKeys.swoPrefix(storeName, s.fieldName, p)
              else TraversalKeys.swPrefix(storeName, s.fieldName, p)
            // If query prefix is longer than prefixMaxLen, ordered-prefix postings are a superset and must be verified;
            // oversample to reduce risk of underfilling the page under IndexOrder.
            val oversample = if query.sort == List(Sort.IndexOrder) && q.length > streamingPrefixMaxLen then streamingIndexOrderPrefixOversample else 1
            TraversalPersistedIndex.postingsStream(prefix, kv)
              .take(limitIntOpt.map(l => l * oversample + offset).getOrElse(Int.MaxValue))
              .map(docId => Id[Doc](docId))
              .evalMap(id => backing.get(id))
              .collect { case Some(doc) => doc }
          }
        }

        def streamedEndsWithDocs(
          kv: lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type],
          e: Filter.EndsWith[Doc, _]
        ): rapid.Stream[Doc] = {
          val q = Option(e.query).getOrElse("")
          if q.isEmpty then rapid.Stream.empty
          else {
            val rev = q.toLowerCase.reverse.take(streamingPrefixMaxLen)
            val prefix =
              if query.sort == List(Sort.IndexOrder) then TraversalKeys.ewoPrefix(storeName, e.fieldName, rev)
              else TraversalKeys.ewPrefix(storeName, e.fieldName, rev)
            val oversample = if query.sort == List(Sort.IndexOrder) && q.length > streamingPrefixMaxLen then streamingIndexOrderPrefixOversample else 1
            TraversalPersistedIndex.postingsStream(prefix, kv)
              .take(limitIntOpt.map(l => l * oversample + offset).getOrElse(Int.MaxValue))
              .map(docId => Id[Doc](docId))
              .evalMap(id => backing.get(id))
              .collect { case Some(doc) => doc }
          }
        }

        def streamedInDocsNoSort(
          kv: lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type],
          in: Filter.In[Doc, _]
        ): Option[rapid.Stream[Doc]] = {
          val values0 = in.values.asInstanceOf[Seq[Any]]
          val values = values0.take(streamingMaxInTerms)

          // Only support simple scalars here (one normalized term per input value).
          val encodedValues: Option[List[String]] = {
            val encoded = values.map { v =>
              TraversalIndex.valuesForIndexValue(v) match {
                case List(null) => Some("null")
                case List(s: String) => Some(s.toLowerCase)
                case List(other) => Some(other.toString)
                case _ => None
              }
            }
            if encoded.forall(_.nonEmpty) then Some(encoded.flatten.toList) else None
          }

          encodedValues match {
            case None => None
            case Some(enc) if enc.isEmpty => None
            case Some(enc) =>
              val prefixes = enc.distinct.map(v => TraversalKeys.eqPrefix(storeName, in.fieldName, v))
              val seen = mutable.HashSet.empty[String]
              val mergedDocIds: rapid.Stream[String] =
                rapid.Stream.merge {
                  Task.pure {
                    val iterator = prefixes.iterator.map { pfx =>
                      TraversalPersistedIndex.postingsStream(pfx, kv)
                    }
                    Pull.fromIterator(iterator)
                  }
                }

              Some(
                mergedDocIds
                  .collect { case docId if seen.add(docId) => docId }
                  .map(docId => Id[Doc](docId))
                  .evalMap(id => backing.get(id))
                  .collect { case Some(doc) => doc }
              )
          }
        }

        def streamedInDocsIndexOrder(
          kv: lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type],
          in: Filter.In[Doc, _],
          required: Int
        ): Task[Option[rapid.Stream[Doc]]] = {
          if required <= 0 then Task.pure(None)
          else if required > streamingMaxInPageDocs then Task.pure(None)
          else {
            val values0 = in.values.asInstanceOf[Seq[Any]]
            val values = values0.take(streamingMaxInTerms)

            val encodedValuesOpt: Option[List[String]] = {
              val encoded = values.map { v =>
                TraversalIndex.valuesForIndexValue(v) match {
                  case List(null) => Some("null")
                  case List(s: String) => Some(s.toLowerCase)
                  case List(other) => Some(other.toString)
                  case _ => None
                }
              }
              if encoded.forall(_.nonEmpty) then Some(encoded.flatten.toList) else None
            }

            encodedValuesOpt match {
              case None =>
                Task.pure(None)
              case Some(enc0) =>
                val enc = enc0.distinct
                if enc.isEmpty then Task.pure(None)
                else {
                  val takeN = required
                  val tasks: List[Task[List[String]]] = enc.map { v =>
                    val pfx = TraversalKeys.eqoPrefix(storeName, in.fieldName, v)
                    TraversalPersistedIndex.postingsTake(pfx, kv, takeN)
                  }
                  tasks.tasks.map { lists =>
                    val ids = TraversalPersistedIndex.mergeSortedDistinctTake(lists, takeN)
                    Some(
                      rapid.Stream
                        .emits(ids)
                        .map(id => Id[Doc](id))
                        .evalMap(id => backing.get(id))
                        .collect { case Some(doc) => doc }
                    )
                  }
                }
            }
          }
        }

        def mergedDocIdsFromPrefixes(
          kv: lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type],
          prefixes: List[String]
        ): rapid.Stream[String] =
          rapid.Stream.merge {
            Task.pure {
              val iterator = prefixes.iterator.map { pfx =>
                TraversalPersistedIndex.postingsStream(pfx, kv)
              }
              Pull.fromIterator(iterator)
            }
          }

        def streamedRangeLongDocsNoSort(
          kv: lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type],
          r: Filter.RangeLong[Doc]
        ): Option[rapid.Stream[Doc]] = {
          val prefixesOpt = TraversalPersistedIndex.rangeLongPrefixesFor(r.from, r.to)
          prefixesOpt match {
            case None => None
            case Some(prefixes0) =>
              val prefixes = prefixes0.distinct.take(streamingMaxRangePrefixes).map(p => TraversalKeys.rlPrefix(storeName, r.fieldName, p))
              if prefixes.isEmpty || prefixes0.size > streamingMaxRangePrefixes then None
              else {
                val seen = mutable.HashSet.empty[String]
                Some(
                  mergedDocIdsFromPrefixes(kv, prefixes)
                    .collect { case docId if seen.add(docId) => docId }
                    .map(docId => Id[Doc](docId))
                    .evalMap(id => backing.get(id))
                    .collect { case Some(doc) => doc }
                )
              }
          }
        }

        def streamedRangeDoubleDocsNoSort(
          kv: lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type],
          r: Filter.RangeDouble[Doc]
        ): Option[rapid.Stream[Doc]] = {
          val prefixesOpt = TraversalPersistedIndex.rangeDoublePrefixesFor(r.from, r.to)
          prefixesOpt match {
            case None => None
            case Some(prefixes0) =>
              val prefixes = prefixes0.distinct.take(streamingMaxRangePrefixes).map(p => TraversalKeys.rdPrefix(storeName, r.fieldName, p))
              if prefixes.isEmpty || prefixes0.size > streamingMaxRangePrefixes then None
              else {
                val seen = mutable.HashSet.empty[String]
                Some(
                  mergedDocIdsFromPrefixes(kv, prefixes)
                    .collect { case docId if seen.add(docId) => docId }
                    .map(docId => Id[Doc](docId))
                    .evalMap(id => backing.get(id))
                    .collect { case Some(doc) => doc }
                )
              }
          }
        }

        def streamedRangeLongDocsIndexOrder(
          kv: lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type],
          r: Filter.RangeLong[Doc],
          required: Int
        ): Task[Option[rapid.Stream[Doc]]] = {
          if required <= 0 then Task.pure(None)
          else if required > streamingMaxRangePageDocs then Task.pure(None)
          else {
            // Use ordered numeric postings with fixed prefix length (low write amp).
            TraversalPersistedIndex.rangeLongOrderedPrefixesFor(r.from, r.to) match {
              case None => Task.pure(None)
              case Some(prefixes0) if prefixes0.isEmpty => Task.pure(None)
              case Some(prefixes0) if prefixes0.size > streamingMaxRangePrefixes => Task.pure(None)
              case Some(prefixes0) =>
                val prefixes = prefixes0.distinct.map(p => TraversalKeys.rloPrefix(storeName, r.fieldName, p))
                val takeN = math.min(streamingMaxRangePageDocs, required * streamingIndexOrderRangeOversample)
                val tasks: List[Task[List[String]]] = prefixes.map { pfx =>
                  TraversalPersistedIndex.postingsTake(pfx, kv, takeN)
                }
                tasks.tasks.map { lists =>
                  val ids = TraversalPersistedIndex.mergeSortedDistinctTake(lists, takeN)
                  if ids.isEmpty then None
                  else {
                    Some(
                      rapid.Stream
                        .emits(ids)
                        .map(id => Id[Doc](id))
                        .evalMap(id => backing.get(id))
                        .collect { case Some(doc) => doc }
                    )
                  }
                }
            }
          }
        }

        def streamedRangeDoubleDocsIndexOrder(
          kv: lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type],
          r: Filter.RangeDouble[Doc],
          required: Int
        ): Task[Option[rapid.Stream[Doc]]] = {
          if required <= 0 then Task.pure(None)
          else if required > streamingMaxRangePageDocs then Task.pure(None)
          else {
            TraversalPersistedIndex.rangeDoubleOrderedPrefixesFor(r.from, r.to) match {
              case None => Task.pure(None)
              case Some(prefixes0) if prefixes0.isEmpty => Task.pure(None)
              case Some(prefixes0) if prefixes0.size > streamingMaxRangePrefixes => Task.pure(None)
              case Some(prefixes0) =>
                val prefixes = prefixes0.distinct.map(p => TraversalKeys.rdoPrefix(storeName, r.fieldName, p))
                val takeN = math.min(streamingMaxRangePageDocs, required * streamingIndexOrderRangeOversample)
                val tasks: List[Task[List[String]]] = prefixes.map { pfx =>
                  TraversalPersistedIndex.postingsTake(pfx, kv, takeN)
                }
                tasks.tasks.map { lists =>
                  val ids = TraversalPersistedIndex.mergeSortedDistinctTake(lists, takeN)
                  if ids.isEmpty then None
                  else {
                    Some(
                      rapid.Stream
                        .emits(ids)
                        .map(id => Id[Doc](id))
                        .evalMap(id => backing.get(id))
                        .collect { case Some(doc) => doc }
                    )
                  }
                }
            }
          }
        }

        def streamedSortByFieldNumericDocs(
          kv: lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type],
          sort: Sort.ByField[Doc, _]
        ): Option[rapid.Stream[Doc]] = {
          val fAny = sort.field.asInstanceOf[Field[Doc, Any]]
          if !fAny.indexed then None
          else {
            // Safety: Optional fields can be null and should sort before non-null in ascending (compareAny),
            // but our persisted order-by postings omit nulls. Only apply this path for non-optional numeric defs.
            def isOpt(d: DefType): Boolean = d match {
              case DefType.Opt(_) => true
              case _ => false
            }

            def isIntDef(d: DefType): Boolean = d match {
              case DefType.Int => true
              case DefType.Opt(inner) => isIntDef(inner)
              case _ => false
            }
            def isDecDef(d: DefType): Boolean = d match {
              case DefType.Dec => true
              case DefType.Opt(inner) => isDecDef(inner)
              case _ => false
            }
            val prefix =
              if !orderByFieldPostingsEnabled || isOpt(fAny.rw.definition) then ""
              else if isIntDef(fAny.rw.definition) then {
                if sort.direction == SortDirection.Ascending then TraversalKeys.olaPrefix(storeName, fAny.name)
                else TraversalKeys.oldPrefix(storeName, fAny.name)
              } else if isDecDef(fAny.rw.definition) then {
                if sort.direction == SortDirection.Ascending then TraversalKeys.odaPrefix(storeName, fAny.name)
                else TraversalKeys.oddPrefix(storeName, fAny.name)
              } else ""

            if prefix.isEmpty then None
            else {
              Some(
                TraversalPersistedIndex.postingsStream(prefix, kv)
                  .map(docId => Id[Doc](docId))
                  .evalMap(id => backing.get(id))
                  .collect { case Some(doc) => doc }
              )
            }
          }
        }

        val (docsStream, approxSeeded, alreadySorted): (rapid.Stream[Doc], Boolean, Boolean) = seedIdsResolved match {
          case Some(ids) =>
            // Deterministic iteration over IDs for stable results when seeding.
            val ordered = ids.toList.sortBy(_.value)
            (rapid.Stream
              .emits(ordered)
              .evalMap(id => backing.get(id))
              .filter(_.nonEmpty)
              .map(_.get), false, false)
          case None =>
            // If we can early-terminate and have a simple Equals filter, stream postings instead of scanning the whole store.
            val canStreamSeed: Boolean =
              streamingSeedEnabled &&
                !query.countTotal &&
                query.facets.isEmpty &&
                limitIntOpt.nonEmpty

            // Special-case: for page-only queries sorted by a numeric field, stream docs in field order using persisted
            // order-by postings and early-terminate once we have offset+limit matches.
            val byFieldSortOpt: Option[Sort.ByField[Doc, _]] =
              if query.sort.size == 1 && query.sort.head.isInstanceOf[Sort.ByField[_, _]] then
                Some(query.sort.head.asInstanceOf[Sort.ByField[Doc, _]])
              else None

            def isOptDef(d: DefType): Boolean = d match {
              case DefType.Opt(_) => true
              case _ => false
            }
            def isIntDef(d: DefType): Boolean = d match {
              case DefType.Int => true
              case DefType.Opt(inner) => isIntDef(inner)
              case _ => false
            }
            def isDecDef(d: DefType): Boolean = d match {
              case DefType.Dec => true
              case DefType.Opt(inner) => isDecDef(inner)
              case _ => false
            }

            val byFieldNumericEligible: Boolean = byFieldSortOpt.exists { sf =>
              val fAny = sf.field.asInstanceOf[Field[Doc, Any]]
              fAny.indexed && !isOptDef(fAny.rw.definition) && (isIntDef(fAny.rw.definition) || isDecDef(fAny.rw.definition))
            }

            val canStreamByField: Boolean =
              streamingSortByFieldEnabled &&
                orderByFieldPostingsEnabled &&
                !query.countTotal &&
                query.facets.isEmpty &&
                limitIntOpt.nonEmpty &&
                byFieldNumericEligible

            (persistedIndex, query.filter) match {
              case (Some(kv), _) if canStreamByField =>
                val sf = byFieldSortOpt.get
                (rapid.Stream.force(
                  TraversalPersistedIndex.isReady(storeName, kv).map { ready =>
                    if !ready then backing.stream
                    else streamedSortByFieldNumericDocs(kv, sf).getOrElse(backing.stream)
                  }
                ), false, true)
              case (Some(kv), Some(e: Filter.Equals[Doc, _]))
                  if canStreamSeed &&
                    (query.sort.isEmpty || query.sort == List(Sort.IndexOrder)) &&
                    e.field(model).indexed &&
                    TraversalIndex.valuesForIndexValue(e.value).size == 1 =>
                // Only use when index is ready; otherwise fallback to full scan.
                (
                  rapid.Stream.force(
                    TraversalPersistedIndex.isReady(storeName, kv).map { ready =>
                      if !ready then backing.stream
                      else {
                        // Tokenized Equals has AND-of-tokens semantics; seed by token postings when applicable.
                        streamedTokenizedEqualsDocs(kv, e).getOrElse(streamedEqualsDocs(kv, e))
                      }
                    }
                  ),
                  // Approximate: tokenized seeding is a superset and bounded by takeN (can underfill without fallback).
                  e.field(model).isTokenized,
                  false
                )
              case (Some(kv), Some(s: Filter.StartsWith[Doc, _]))
                  if canStreamSeed &&
                    (query.sort.isEmpty || query.sort == List(Sort.IndexOrder)) &&
                    s.field(model).indexed &&
                    Option(s.query).exists(_.nonEmpty) =>
                (
                  rapid.Stream.force(
                  TraversalPersistedIndex.isReady(storeName, kv).map { ready =>
                    if ready then streamedStartsWithDocs(kv, s)
                    else backing.stream
                  }
                ),
                  // Approximate only when IndexOrder and query is longer than persisted prefixMaxLen (must verify + truncated take).
                  query.sort == List(Sort.IndexOrder) && Option(s.query).exists(_.length > streamingPrefixMaxLen),
                  false
                )
              case (Some(kv), Some(e: Filter.EndsWith[Doc, _]))
                  if canStreamSeed &&
                    (query.sort.isEmpty || query.sort == List(Sort.IndexOrder)) &&
                    e.field(model).indexed &&
                    Option(e.query).exists(_.nonEmpty) =>
                (
                  rapid.Stream.force(
                  TraversalPersistedIndex.isReady(storeName, kv).map { ready =>
                    if ready then streamedEndsWithDocs(kv, e)
                    else backing.stream
                  }
                ),
                  query.sort == List(Sort.IndexOrder) && Option(e.query).exists(_.length > streamingPrefixMaxLen),
                  false
                )
              case (Some(kv), Some(i: Filter.In[Doc, _]))
                  if canStreamSeed &&
                    i.field(model).indexed &&
                    i.values.asInstanceOf[Seq[Any]].nonEmpty &&
                    i.values.asInstanceOf[Seq[Any]].size <= streamingMaxInTerms =>
                val required = offset + limitIntOpt.getOrElse(0)
                if query.sort == List(Sort.IndexOrder) then {
                  (rapid.Stream.force(
                    TraversalPersistedIndex.isReady(storeName, kv).flatMap { ready =>
                      if !ready then Task.pure(backing.stream)
                      else streamedInDocsIndexOrder(kv, i, required).map(_.getOrElse(backing.stream))
                    }
                  ), false, false)
                } else if query.sort.isEmpty then {
                  (rapid.Stream.force(
                    TraversalPersistedIndex.isReady(storeName, kv).map { ready =>
                      if ready then streamedInDocsNoSort(kv, i).getOrElse(backing.stream)
                      else backing.stream
                    }
                  ), false, false)
                } else (backing.stream, false, false)
              case (Some(kv), Some(r: Filter.RangeLong[Doc]))
                  if canStreamSeed &&
                    limitIntOpt.nonEmpty &&
                    r.field(model).indexed =>
                val required = offset + limitIntOpt.getOrElse(0)
                if query.sort == List(Sort.IndexOrder) then {
                  (rapid.Stream.force(
                    TraversalPersistedIndex.isReady(storeName, kv).flatMap { ready =>
                      if !ready then Task.pure(backing.stream)
                      else streamedRangeLongDocsIndexOrder(kv, r, required).map(_.getOrElse(backing.stream))
                    }
                  ), true, false) // approximate due to coarse numeric buckets + bounded take
                } else if query.sort.isEmpty then {
                  (rapid.Stream.force(
                    TraversalPersistedIndex.isReady(storeName, kv).map { ready =>
                      if !ready then backing.stream
                      else streamedRangeLongDocsNoSort(kv, r).getOrElse(backing.stream)
                    }
                  ), false, false)
                } else (backing.stream, false, false)
              case (Some(kv), Some(r: Filter.RangeDouble[Doc]))
                  if canStreamSeed &&
                    limitIntOpt.nonEmpty &&
                    r.field(model).indexed =>
                val required = offset + limitIntOpt.getOrElse(0)
                if query.sort == List(Sort.IndexOrder) then {
                  (rapid.Stream.force(
                    TraversalPersistedIndex.isReady(storeName, kv).flatMap { ready =>
                      if !ready then Task.pure(backing.stream)
                      else streamedRangeDoubleDocsIndexOrder(kv, r, required).map(_.getOrElse(backing.stream))
                    }
                  ), true, false)
                } else if query.sort.isEmpty then {
                  (rapid.Stream.force(
                    TraversalPersistedIndex.isReady(storeName, kv).map { ready =>
                      if !ready then backing.stream
                      else streamedRangeDoubleDocsNoSort(kv, r).getOrElse(backing.stream)
                    }
                  ), false, false)
                } else (backing.stream, false, false)
              case (Some(kv), Some(m: Filter.Multi[Doc])) if canStreamSeed && limitIntOpt.nonEmpty =>
                // Streaming seed for common chained filters:
                // pick one MUST/FILTER clause as the driver stream, then verify the full filter.
                val streamingMultiIntersectIndexOrderEnabled: Boolean =
                  Profig("lightdb.traversal.persistedIndex.streamingSeed.multi.intersectIndexOrder.enabled").opt[Boolean].getOrElse(true)
                val streamingMultiIntersectIndexOrderMaxStreams: Int =
                  Profig("lightdb.traversal.persistedIndex.streamingSeed.multi.intersectIndexOrder.maxStreams").opt[Int].getOrElse(4) max 2
                val streamingMultiRefineEnabled: Boolean =
                  Profig("lightdb.traversal.persistedIndex.streamingSeed.multi.refine.enabled").opt[Boolean].getOrElse(true)
                val streamingMultiRefineExcludeEnabled: Boolean =
                  Profig("lightdb.traversal.persistedIndex.streamingSeed.multi.refineExclude.enabled").opt[Boolean].getOrElse(true)
                val streamingMultiRefineMaxIds: Int =
                  Profig("lightdb.traversal.persistedIndex.streamingSeed.multi.refine.maxIds").opt[Int].getOrElse(50_000) max 1
                val streamingMultiRefineMaxClauses: Int =
                  Profig("lightdb.traversal.persistedIndex.streamingSeed.multi.refine.maxClauses").opt[Int].getOrElse(2) max 0

                val driverClauses: List[Filter[Doc]] = m.filters.collect {
                  case fc if fc.condition == Condition.Must || fc.condition == Condition.Filter =>
                    fc.filter.asInstanceOf[Filter[Doc]]
                }
                val shouldClauses: List[Filter[Doc]] = m.filters.collect {
                  case fc if fc.condition == Condition.Should => fc.filter.asInstanceOf[Filter[Doc]]
                }
                val mustNotClauses: List[Filter[Doc]] = m.filters.collect {
                  case fc if fc.condition == Condition.MustNot => fc.filter.asInstanceOf[Filter[Doc]]
                }

                def tokenizedTokensForEquals(e: Filter.Equals[Doc, _]): List[String] = {
                  if !e.field(model).isTokenized then Nil
                  else {
                    e.getJson(model) match {
                      case fabric.Str(s, _) =>
                        Option(s)
                          .getOrElse("")
                          .toLowerCase
                          .split("\\s+")
                          .toList
                          .map(_.trim)
                          .filter(_.nonEmpty)
                          .distinct
                      case _ =>
                        Nil
                    }
                  }
                }

                def isStreamingSeedable(f: Filter[Doc]): Boolean = f match {
                  case e: Filter.Equals[Doc, _] =>
                    e.field(model).indexed &&
                      TraversalIndex.valuesForIndexValue(e.value).size == 1 && (
                        !e.field(model).isTokenized || tokenizedTokensForEquals(e).nonEmpty
                      )
                  case s: Filter.StartsWith[Doc, _] => s.field(model).indexed && Option(s.query).exists(_.nonEmpty)
                  case e: Filter.EndsWith[Doc, _] => e.field(model).indexed && Option(e.query).exists(_.nonEmpty)
                  case i: Filter.In[Doc, _] =>
                    i.field(model).indexed &&
                      i.values.asInstanceOf[Seq[Any]].nonEmpty &&
                      i.values.asInstanceOf[Seq[Any]].size <= streamingMaxInTerms
                  case r: Filter.RangeLong[Doc] => r.field(model).indexed
                  case r: Filter.RangeDouble[Doc] => r.field(model).indexed
                  case _ => false
                }

                // Prefer a clause that matches the requested order, since it may have a cheaper / ordered streaming path.
                def priority(f: Filter[Doc]): Int = (query.sort, f) match {
                  case (List(Sort.IndexOrder), _: Filter.Equals[Doc, _]) => 0
                  case (List(Sort.IndexOrder), _: Filter.StartsWith[Doc, _]) => 1
                  case (List(Sort.IndexOrder), _: Filter.EndsWith[Doc, _]) => 1
                  case (List(Sort.IndexOrder), _: Filter.In[Doc, _]) => 2
                  case (List(Sort.IndexOrder), _: Filter.RangeLong[Doc]) => 3
                  case (List(Sort.IndexOrder), _: Filter.RangeDouble[Doc]) => 3
                  case (_, _: Filter.Equals[Doc, _]) => 0
                  case (_, _: Filter.StartsWith[Doc, _]) => 1
                  case (_, _: Filter.EndsWith[Doc, _]) => 1
                  case (_, _: Filter.In[Doc, _]) => 2
                  case (_, _: Filter.RangeLong[Doc]) => 3
                  case (_, _: Filter.RangeDouble[Doc]) => 3
                  case _ => 9
                }

                val seedable = driverClauses.filter(isStreamingSeedable)

                def driverCountHint(f: Filter[Doc]): Task[Option[Int]] = f match {
                  case e: Filter.Equals[Doc, _] =>
                    if e.field(model).isTokenized then {
                      val tokens = tokenizedTokensForEquals(e)
                      tokens.headOption match {
                        case None => Task.pure(None)
                        case Some(token0) =>
                          // Hint using the smallest token postings count (bounded), since we'd seed by a single token.
                          val prefixes = tokens.map { tok =>
                            if query.sort == List(Sort.IndexOrder) then TraversalKeys.tokoPrefix(storeName, e.fieldName, tok)
                            else TraversalKeys.tokPrefix(storeName, e.fieldName, tok)
                          }
                          prefixes
                            .map(p => TraversalPersistedIndex.postingsTakeAndCountUpTo(p, kv, 1, streamingMultiDriverMaxCount).map { case (_, c) => p -> c })
                            .tasks
                            .map { pcs =>
                              val best = pcs.minByOption(_._2)
                              best.flatMap { case (_, c) =>
                                if c > streamingMultiDriverMaxCount then None else Some(c)
                              }
                            }
                      }
                    } else {
                      val raw = TraversalIndex.valuesForIndexValue(e.value)
                      val enc = raw.headOption.map {
                        case null => "null"
                        case s: String => s.toLowerCase
                        case v => v.toString
                      }.getOrElse("")
                      val prefix =
                        if query.sort == List(Sort.IndexOrder) then TraversalKeys.eqoPrefix(storeName, e.fieldName, enc)
                        else TraversalKeys.eqPrefix(storeName, e.fieldName, enc)
                      TraversalPersistedIndex.postingsTakeAndCountUpTo(prefix, kv, 1, streamingMultiDriverMaxCount).map { case (_, c) =>
                        if c > streamingMultiDriverMaxCount then None else Some(c)
                      }
                    }

                  case s: Filter.StartsWith[Doc, _] =>
                    val pOpt = Option(s.query).map(_.toLowerCase.take(streamingPrefixMaxLen)).filter(_.nonEmpty)
                    pOpt match {
                      case None => Task.pure(None)
                      case Some(p) =>
                        val prefix =
                          if query.sort == List(Sort.IndexOrder) then TraversalKeys.swoPrefix(storeName, s.fieldName, p)
                          else TraversalKeys.swPrefix(storeName, s.fieldName, p)
                        TraversalPersistedIndex.postingsTakeAndCountUpTo(prefix, kv, 1, streamingMultiDriverMaxCount).map { case (_, c) =>
                          if c > streamingMultiDriverMaxCount then None else Some(c)
                        }
                    }

                  case e: Filter.EndsWith[Doc, _] =>
                    val pOpt = Option(e.query).map(_.toLowerCase.reverse.take(streamingPrefixMaxLen)).filter(_.nonEmpty)
                    pOpt match {
                      case None => Task.pure(None)
                      case Some(p) =>
                        val prefix =
                          if query.sort == List(Sort.IndexOrder) then TraversalKeys.ewoPrefix(storeName, e.fieldName, p)
                          else TraversalKeys.ewPrefix(storeName, e.fieldName, p)
                        TraversalPersistedIndex.postingsTakeAndCountUpTo(prefix, kv, 1, streamingMultiDriverMaxCount).map { case (_, c) =>
                          if c > streamingMultiDriverMaxCount then None else Some(c)
                        }
                    }

                  case i: Filter.In[Doc, _] =>
                    // Upper bound: sum per-term postings counts (overcounts due to overlaps, but safe as a hint).
                    val values0 = i.values.asInstanceOf[Seq[Any]].take(streamingMaxInTerms)
                    val encodedValuesOpt: Option[List[String]] = {
                      val encoded = values0.map { v =>
                        TraversalIndex.valuesForIndexValue(v) match {
                          case List(null) => Some("null")
                          case List(s: String) => Some(s.toLowerCase)
                          case List(other) => Some(other.toString)
                          case _ => None
                        }
                      }
                      if encoded.forall(_.nonEmpty) then Some(encoded.flatten.toList.distinct) else None
                    }
                    encodedValuesOpt match {
                      case None => Task.pure(None)
                      case Some(enc) if enc.isEmpty => Task.pure(None)
                      case Some(enc) =>
                        val prefixes = enc.map { v =>
                          if query.sort == List(Sort.IndexOrder) then TraversalKeys.eqoPrefix(storeName, i.fieldName, v)
                          else TraversalKeys.eqPrefix(storeName, i.fieldName, v)
                        }
                        // Sum counts up to max; bail if it exceeds max.
                        prefixes
                          .foldLeft(Task.pure(0)) { (accT, pfx) =>
                            accT.flatMap { acc =>
                              if acc > streamingMultiDriverMaxCount then Task.pure(acc)
                              else {
                                val remaining = (streamingMultiDriverMaxCount - acc) max 0
                                TraversalPersistedIndex.postingsTakeAndCountUpTo(pfx, kv, 1, remaining).map { case (_, c) => c + acc }
                              }
                            }
                          }
                          .map { sum =>
                            if sum > streamingMultiDriverMaxCount then None else Some(sum)
                          }
                    }

                  case r: Filter.RangeLong[Doc] =>
                    val prefixesOpt =
                      if query.sort == List(Sort.IndexOrder) then TraversalPersistedIndex.rangeLongOrderedPrefixesFor(r.from, r.to)
                      else TraversalPersistedIndex.rangeLongPrefixesFor(r.from, r.to)
                    prefixesOpt match {
                      case None => Task.pure(None)
                      case Some(ps0) =>
                        val ps = ps0.distinct.take(streamingMultiDriverMaxCountPrefixes)
                        if ps.isEmpty || ps0.size > streamingMultiDriverMaxCountPrefixes then Task.pure(None)
                        else {
                          val prefixes =
                            if query.sort == List(Sort.IndexOrder) then ps.map(p => TraversalKeys.rloPrefix(storeName, r.fieldName, p))
                            else ps.map(p => TraversalKeys.rlPrefix(storeName, r.fieldName, p))
                          prefixes
                            .foldLeft(Task.pure(0)) { (accT, pfx) =>
                              accT.flatMap { acc =>
                                if acc > streamingMultiDriverMaxCount then Task.pure(acc)
                                else {
                                  val remaining = (streamingMultiDriverMaxCount - acc) max 0
                                  TraversalPersistedIndex.postingsTakeAndCountUpTo(pfx, kv, 1, remaining).map { case (_, c) => c + acc }
                                }
                              }
                            }
                            .map { sum =>
                              if sum > streamingMultiDriverMaxCount then None else Some(sum)
                            }
                        }
                    }

                  case r: Filter.RangeDouble[Doc] =>
                    val prefixesOpt =
                      if query.sort == List(Sort.IndexOrder) then TraversalPersistedIndex.rangeDoubleOrderedPrefixesFor(r.from, r.to)
                      else TraversalPersistedIndex.rangeDoublePrefixesFor(r.from, r.to)
                    prefixesOpt match {
                      case None => Task.pure(None)
                      case Some(ps0) =>
                        val ps = ps0.distinct.take(streamingMultiDriverMaxCountPrefixes)
                        if ps.isEmpty || ps0.size > streamingMultiDriverMaxCountPrefixes then Task.pure(None)
                        else {
                          val prefixes =
                            if query.sort == List(Sort.IndexOrder) then ps.map(p => TraversalKeys.rdoPrefix(storeName, r.fieldName, p))
                            else ps.map(p => TraversalKeys.rdPrefix(storeName, r.fieldName, p))
                          prefixes
                            .foldLeft(Task.pure(0)) { (accT, pfx) =>
                              accT.flatMap { acc =>
                                if acc > streamingMultiDriverMaxCount then Task.pure(acc)
                                else {
                                  val remaining = (streamingMultiDriverMaxCount - acc) max 0
                                  TraversalPersistedIndex.postingsTakeAndCountUpTo(pfx, kv, 1, remaining).map { case (_, c) => c + acc }
                                }
                              }
                            }
                            .map { sum =>
                              if sum > streamingMultiDriverMaxCount then None else Some(sum)
                            }
                        }
                    }

                  case _ =>
                    Task.pure(None)
                }

                // Use a bounded postings-count heuristic when we can compute it cheaply; otherwise fallback to priority only.
                def pickDriverReady(): Task[Option[Filter[Doc]]] = {
                  if seedable.isEmpty then Task.pure(None)
                  else {
                    val withCounts: List[Task[(Filter[Doc], Option[Int])]] =
                      seedable.map(f => driverCountHint(f).map(c => f -> c))
                    withCounts.tasks.map { pairs =>
                      // Prefer known smallest count; if unknown, use priority ordering.
                      val known = pairs.collect { case (f, Some(c)) => f -> c }
                      if known.nonEmpty then Some(known.minBy { case (f, c) => (c, priority(f)) }._1)
                      else seedable.sortBy(priority).headOption
                    }
                  }
                }

                // Return a stream based on the selected driver. Mark as approximate for safety, since Multi verification
                // can underfill pages due to additional constraints (fallback will fill via scan when needed).
                val required = offset + limitIntOpt.getOrElse(0)
                val streamTask: Task[rapid.Stream[Doc]] =
                  TraversalPersistedIndex.isReady(storeName, kv).flatMap { ready =>
                    if !ready then Task.pure(backing.stream)
                    else {
                      def orderedPrefixesForClauseIndexOrder(f: Filter[Doc]): List[String] = f match {
                        case e: Filter.Equals[Doc, _] if e.field(model).indexed =>
                          if e.field(model).isTokenized then {
                            val tokens = tokenizedTokensForEquals(e)
                            tokens.map(tok => TraversalKeys.tokoPrefix(storeName, e.fieldName, tok))
                          } else {
                            val raw = TraversalIndex.valuesForIndexValue(e.value)
                            if raw.size != 1 then Nil
                            else {
                              val enc = raw.headOption.map {
                                case null => "null"
                                case s: String => s.toLowerCase
                                case v => v.toString
                              }.getOrElse("")
                              List(TraversalKeys.eqoPrefix(storeName, e.fieldName, enc))
                            }
                          }
                        case s: Filter.StartsWith[Doc, _] if s.field(model).indexed =>
                          Option(s.query).map(_.toLowerCase.take(streamingPrefixMaxLen)).filter(_.nonEmpty).toList.map { p =>
                            TraversalKeys.swoPrefix(storeName, s.fieldName, p)
                          }
                        case e: Filter.EndsWith[Doc, _] if e.field(model).indexed =>
                          Option(e.query).map(_.toLowerCase.reverse.take(streamingPrefixMaxLen)).filter(_.nonEmpty).toList.map { p =>
                            TraversalKeys.ewoPrefix(storeName, e.fieldName, p)
                          }
                        case _ =>
                          Nil
                      }

                      // IndexOrder optimization: if we have multiple MUST/FILTER clauses that can produce docId-ordered postings,
                      // intersect their postings streams first (streaming) to drastically reduce doc fetch/verify work.
                      val intersectStreamT: Task[Option[rapid.Stream[Doc]]] = {
                        if !streamingMultiIntersectIndexOrderEnabled || query.sort != List(Sort.IndexOrder) then Task.pure(None)
                        else {
                          val takeN = (required * streamingIndexOrderPrefixOversample) max 0
                          if takeN <= 0 then Task.pure(None)
                          else if takeN > streamingMultiDriverMaxCount then Task.pure(None) // avoid overly large scans per clause
                          else {
                            def idsForClauseIndexOrder(f: Filter[Doc]): Task[Option[(List[String], Int)]] = f match {
                              case e: Filter.Equals[Doc, _] if e.field(model).indexed =>
                                if e.field(model).isTokenized then {
                                  val tokens = tokenizedTokensForEquals(e)
                                  if tokens.isEmpty then Task.pure(None)
                                  else {
                                    val prefixes = tokens.map(tok => TraversalKeys.tokoPrefix(storeName, e.fieldName, tok))
                                    prefixes
                                      .map(p => TraversalPersistedIndex.postingsTakeAndCountUpTo(p, kv, takeN, streamingMultiDriverMaxCount).map { case (ids, c) => (ids, c) })
                                      .tasks
                                      .map { pairs =>
                                        val best = pairs.minBy { case (ids, c) => (c, ids.size) }
                                        Some(best)
                                      }
                                  }
                                } else {
                                  val raw = TraversalIndex.valuesForIndexValue(e.value)
                                  if raw.size != 1 then Task.pure(None)
                                  else {
                                    val enc = raw.headOption.map {
                                      case null => "null"
                                      case s: String => s.toLowerCase
                                      case v => v.toString
                                    }.getOrElse("")
                                    TraversalPersistedIndex
                                      .postingsTakeAndCountUpTo(TraversalKeys.eqoPrefix(storeName, e.fieldName, enc), kv, takeN, streamingMultiDriverMaxCount)
                                      .map { case (ids, c) => Some(ids -> c) }
                                  }
                                }

                              case s: Filter.StartsWith[Doc, _] if s.field(model).indexed =>
                                Option(s.query).map(_.toLowerCase.take(streamingPrefixMaxLen)).filter(_.nonEmpty) match {
                                  case None => Task.pure(None)
                                  case Some(p) =>
                                    TraversalPersistedIndex
                                      .postingsTakeAndCountUpTo(TraversalKeys.swoPrefix(storeName, s.fieldName, p), kv, takeN, streamingMultiDriverMaxCount)
                                      .map { case (ids, c) => Some(ids -> c) }
                                }

                              case e: Filter.EndsWith[Doc, _] if e.field(model).indexed =>
                                Option(e.query).map(_.toLowerCase.reverse.take(streamingPrefixMaxLen)).filter(_.nonEmpty) match {
                                  case None => Task.pure(None)
                                  case Some(p) =>
                                    TraversalPersistedIndex
                                      .postingsTakeAndCountUpTo(TraversalKeys.ewoPrefix(storeName, e.fieldName, p), kv, takeN, streamingMultiDriverMaxCount)
                                      .map { case (ids, c) => Some(ids -> c) }
                                }

                              case i: Filter.In[Doc, _] if i.field(model).indexed =>
                                val values0 = i.values.asInstanceOf[Seq[Any]].take(streamingMaxInTerms).toList
                                val encodedOpt: Option[List[String]] = {
                                  val encoded = values0.map { v =>
                                    TraversalIndex.valuesForIndexValue(v) match {
                                      case List(null) => Some("null")
                                      case List(s: String) => Some(s.toLowerCase)
                                      case List(other) => Some(other.toString)
                                      case _ => None
                                    }
                                  }
                                  if values0.nonEmpty && encoded.forall(_.nonEmpty) then Some(encoded.flatten.distinct) else None
                                }
                                encodedOpt match {
                                  case None => Task.pure(None)
                                  case Some(enc) if enc.isEmpty => Task.pure(None)
                                  case Some(enc) =>
                                    enc
                                      .map(v => TraversalPersistedIndex.postingsTake(TraversalKeys.eqoPrefix(storeName, i.fieldName, v), kv, takeN))
                                      .tasks
                                      .map { lists =>
                                        val ids = TraversalPersistedIndex.mergeSortedDistinctTake(lists, takeN)
                                        if ids.isEmpty then None else Some(ids -> ids.size)
                                      }
                                }

                              case r: Filter.RangeLong[Doc] if r.field(model).indexed =>
                                TraversalPersistedIndex.rangeLongOrderedPrefixesFor(r.from, r.to) match {
                                  case None => Task.pure(None)
                                  case Some(prefixes0) if prefixes0.isEmpty => Task.pure(None)
                                  case Some(prefixes0) if prefixes0.size > streamingMaxRangePrefixes => Task.pure(None)
                                  case Some(prefixes0) =>
                                    val prefixes = prefixes0.distinct.map(p => TraversalKeys.rloPrefix(storeName, r.fieldName, p))
                                    prefixes
                                      .map(pfx => TraversalPersistedIndex.postingsTake(pfx, kv, takeN))
                                      .tasks
                                      .map { lists =>
                                        val ids = TraversalPersistedIndex.mergeSortedDistinctTake(lists, takeN)
                                        if ids.isEmpty then None else Some(ids -> ids.size)
                                      }
                                }

                              case r: Filter.RangeDouble[Doc] if r.field(model).indexed =>
                                TraversalPersistedIndex.rangeDoubleOrderedPrefixesFor(r.from, r.to) match {
                                  case None => Task.pure(None)
                                  case Some(prefixes0) if prefixes0.isEmpty => Task.pure(None)
                                  case Some(prefixes0) if prefixes0.size > streamingMaxRangePrefixes => Task.pure(None)
                                  case Some(prefixes0) =>
                                    val prefixes = prefixes0.distinct.map(p => TraversalKeys.rdoPrefix(storeName, r.fieldName, p))
                                    prefixes
                                      .map(pfx => TraversalPersistedIndex.postingsTake(pfx, kv, takeN))
                                      .tasks
                                      .map { lists =>
                                        val ids = TraversalPersistedIndex.mergeSortedDistinctTake(lists, takeN)
                                        if ids.isEmpty then None else Some(ids -> ids.size)
                                      }
                                }

                              case _ =>
                                Task.pure(None)
                            }

                            driverClauses
                              .map(idsForClauseIndexOrder)
                              .tasks
                              .map(_.flatten)
                              .flatMap { clauseLists =>
                                val nonEmpty = clauseLists.filter(_._1.nonEmpty)
                                if nonEmpty.size < 2 then Task.pure(None)
                                else {
                                  val selected = nonEmpty.sortBy { case (ids, hint) => (hint, ids.size) }.take(streamingMultiIntersectIndexOrderMaxStreams)
                                  if selected.size < 2 then Task.pure(None)
                                  else {
                                    val baseIds = TraversalPersistedIndex.intersectSortedDistinctTake(selected.map(_._1), takeN)
                                    if baseIds.isEmpty then Task.pure(None)
                                    else if !streamingMultiRefineExcludeEnabled || mustNotClauses.isEmpty then {
                                      Task.pure(
                                        Some(
                                          rapid.Stream
                                            .emits(baseIds)
                                            .map(docId => Id[Doc](docId))
                                            .evalMap(id => backing.get(id))
                                            .collect { case Some(doc) => doc }
                                        )
                                      )
                                    } else {
                                      // Best-effort exclusion refinement (correctness-safe): subtract small MUST_NOT postings sets
                                      // BEFORE fetching docs. Exclusions must be exact; otherwise we'd drop valid docs.
                                      def excludePrefixOpt(f: Filter[Doc]): Option[String] = f match {
                                        case e: Filter.Equals[Doc, _] =>
                                          TraversalIndex.valuesForIndexValue(e.value) match {
                                            case List(_: String) => None
                                            case List(_) =>
                                              val raw = TraversalIndex.valuesForIndexValue(e.value)
                                              val enc = raw.headOption.map {
                                                case null => "null"
                                                case s: String => s.toLowerCase
                                                case v => v.toString
                                              }.getOrElse("")
                                              Some(TraversalKeys.eqoPrefix(storeName, e.fieldName, enc))
                                            case _ => None
                                          }
                                        case ne: Filter.NotEquals[Doc, _] =>
                                          TraversalIndex.valuesForIndexValue(ne.value) match {
                                            case List(_: String) => None
                                            case List(_) =>
                                              val raw = TraversalIndex.valuesForIndexValue(ne.value)
                                              val enc = raw.headOption.map {
                                                case null => "null"
                                                case s: String => s.toLowerCase
                                                case v => v.toString
                                              }.getOrElse("")
                                              Some(TraversalKeys.eqoPrefix(storeName, ne.fieldName, enc))
                                            case _ => None
                                          }
                                        case s: Filter.StartsWith[Doc, _] =>
                                          Option(s.query)
                                            .filter(_.nonEmpty)
                                            .filter(_.length <= streamingPrefixMaxLen)
                                            .map(_.toLowerCase.take(streamingPrefixMaxLen))
                                            .filter(_.nonEmpty)
                                            .map(p => TraversalKeys.swoPrefix(storeName, s.fieldName, p))
                                        case e: Filter.EndsWith[Doc, _] =>
                                          Option(e.query)
                                            .filter(_.nonEmpty)
                                            .filter(_.length <= streamingPrefixMaxLen)
                                            .map(_.toLowerCase.reverse.take(streamingPrefixMaxLen))
                                            .filter(_.nonEmpty)
                                            .map(p => TraversalKeys.ewoPrefix(storeName, e.fieldName, p))
                                        case _ =>
                                          None
                                      }

                                      val excludePrefixes =
                                        mustNotClauses.flatMap(excludePrefixOpt).distinct.take(streamingMultiRefineMaxClauses)
                                      if excludePrefixes.isEmpty then Task.pure(
                                        Some(
                                          rapid.Stream
                                            .emits(baseIds)
                                            .map(docId => Id[Doc](docId))
                                            .evalMap(id => backing.get(id))
                                            .collect { case Some(doc) => doc }
                                        )
                                      )
                                      else {
                                        def smallSet(pfx: String): Task[Option[Set[String]]] =
                                          TraversalPersistedIndex.postingsTakeAndCountUpTo(pfx, kv, streamingMultiRefineMaxIds + 1, streamingMultiRefineMaxIds + 1).map {
                                            case (ids, c) => if c <= streamingMultiRefineMaxIds then Some(ids.toSet) else None
                                          }

                                        excludePrefixes.map(smallSet).tasks.map { sets =>
                                          val exclude = sets.flatten.foldLeft(Set.empty[String])(_ union _)
                                          val filtered =
                                            if exclude.isEmpty then baseIds
                                            else baseIds.filterNot(exclude.contains)
                                          Some(
                                            rapid.Stream
                                              .emits(filtered)
                                              .map(docId => Id[Doc](docId))
                                              .evalMap(id => backing.get(id))
                                              .collect { case Some(doc) => doc }
                                          )
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                          }
                        }
                      }

                      def streamForClause(f: Filter[Doc]): Task[rapid.Stream[Doc]] = f match {
                        case e: Filter.Equals[Doc, _] =>
                          if e.field(model).isTokenized then {
                            val tokens = tokenizedTokensForEquals(e)
                            if tokens.isEmpty then Task.pure(backing.stream)
                            else {
                              val prefixes = tokens.map { tok =>
                                if query.sort == List(Sort.IndexOrder) then TraversalKeys.tokoPrefix(storeName, e.fieldName, tok)
                                else TraversalKeys.tokPrefix(storeName, e.fieldName, tok)
                              }
                              // Seed by the smallest token postings stream (best-effort).
                              prefixes
                                .map(p => TraversalPersistedIndex.postingsTakeAndCountUpTo(p, kv, 1, streamingMultiDriverMaxCount).map { case (_, c) => p -> c })
                                .tasks
                                .map { pcs =>
                                  val bestPrefix = pcs.minBy(_._2)._1
                                  // Page-only bound: avoid scanning huge token postings when verification is selective.
                                  val takeN =
                                    if query.sort == List(Sort.IndexOrder) then (required * streamingIndexOrderPrefixOversample + offset) max 0
                                    else Int.MaxValue
                                  if takeN == Int.MaxValue then {
                                    TraversalPersistedIndex
                                      .postingsStream(bestPrefix, kv)
                                      .map(docId => Id[Doc](docId))
                                      .evalMap(id => backing.get(id))
                                      .collect { case Some(doc) => doc }
                                  } else {
                                    rapid.Stream.force(
                                      TraversalPersistedIndex.postingsTakeAndCountUpTo(bestPrefix, kv, takeN, streamingMultiDriverMaxCount).map {
                                        case (ids, _) =>
                                          rapid.Stream
                                            .emits(ids)
                                            .map(docId => Id[Doc](docId))
                                            .evalMap(id => backing.get(id))
                                            .collect { case Some(doc) => doc }
                                      }
                                    )
                                  }
                                }
                            }
                          } else Task.pure(streamedEqualsDocs(kv, e))
                        case s: Filter.StartsWith[Doc, _] =>
                          Task.pure(streamedStartsWithDocs(kv, s))
                        case e: Filter.EndsWith[Doc, _] =>
                          Task.pure(streamedEndsWithDocs(kv, e))
                        case i: Filter.In[Doc, _] if query.sort == List(Sort.IndexOrder) =>
                          streamedInDocsIndexOrder(kv, i, required).map(_.getOrElse(backing.stream))
                        case i: Filter.In[Doc, _] if query.sort.isEmpty =>
                          Task.pure(streamedInDocsNoSort(kv, i).getOrElse(backing.stream))
                        case r: Filter.RangeLong[Doc] if query.sort == List(Sort.IndexOrder) =>
                          streamedRangeLongDocsIndexOrder(kv, r, required).map(_.getOrElse(backing.stream))
                        case r: Filter.RangeLong[Doc] if query.sort.isEmpty =>
                          Task.pure(streamedRangeLongDocsNoSort(kv, r).getOrElse(backing.stream))
                        case r: Filter.RangeDouble[Doc] if query.sort == List(Sort.IndexOrder) =>
                          streamedRangeDoubleDocsIndexOrder(kv, r, required).map(_.getOrElse(backing.stream))
                        case r: Filter.RangeDouble[Doc] if query.sort.isEmpty =>
                          Task.pure(streamedRangeDoubleDocsNoSort(kv, r).getOrElse(backing.stream))
                        case _ =>
                          Task.pure(backing.stream)
                      }

                      intersectStreamT.flatMap { sOpt =>
                        sOpt match {
                          case Some(s) =>
                            Task.pure(s)
                          case None =>
                            pickDriverReady().flatMap {
                              case Some(driver) =>
                                streamForClause(driver).flatMap { baseStream0 =>
                                  if !streamingMultiRefineEnabled || streamingMultiRefineMaxClauses == 0 then Task.pure(baseStream0)
                                  else {
                                    // Best-effort refinement: intersect the driver stream with additional small MUST/FILTER postings sets
                                    // before fetching/verifying docs. This is bounded and correctness-safe (it only narrows candidates).
                                    def refinePrefixOpt(f: Filter[Doc]): Option[String] = f match {
                                      case e: Filter.Equals[Doc, _] =>
                                        val raw = TraversalIndex.valuesForIndexValue(e.value)
                                        val enc = raw.headOption.map {
                                          case null => "null"
                                          case s: String => s.toLowerCase
                                          case v => v.toString
                                        }.getOrElse("")
                                        Some(
                                          if query.sort == List(Sort.IndexOrder) then TraversalKeys.eqoPrefix(storeName, e.fieldName, enc)
                                          else TraversalKeys.eqPrefix(storeName, e.fieldName, enc)
                                        )
                                      case s: Filter.StartsWith[Doc, _] =>
                                        Option(s.query).map(_.toLowerCase.take(streamingPrefixMaxLen)).filter(_.nonEmpty).map { p =>
                                          if query.sort == List(Sort.IndexOrder) then TraversalKeys.swoPrefix(storeName, s.fieldName, p)
                                          else TraversalKeys.swPrefix(storeName, s.fieldName, p)
                                        }
                                      case e: Filter.EndsWith[Doc, _] =>
                                        Option(e.query).map(_.toLowerCase.reverse.take(streamingPrefixMaxLen)).filter(_.nonEmpty).map { p =>
                                          if query.sort == List(Sort.IndexOrder) then TraversalKeys.ewoPrefix(storeName, e.fieldName, p)
                                          else TraversalKeys.ewPrefix(storeName, e.fieldName, p)
                                        }
                                      case ne: Filter.NotEquals[Doc, _] =>
                                        // Exclusion refinement only: same prefix as Equals, but used to subtract.
                                        val raw = TraversalIndex.valuesForIndexValue(ne.value)
                                        if raw.size != 1 then None
                                        else {
                                          val enc = raw.headOption.map {
                                            case null => "null"
                                            case s: String => s.toLowerCase
                                            case v => v.toString
                                          }.getOrElse("")
                                          Some(
                                            if query.sort == List(Sort.IndexOrder) then TraversalKeys.eqoPrefix(storeName, ne.fieldName, enc)
                                            else TraversalKeys.eqPrefix(storeName, ne.fieldName, enc)
                                          )
                                        }
                                      case _ => None
                                    }

                                    val includeCandidates0 =
                                      seedable.filterNot(_ == driver).flatMap(f => refinePrefixOpt(f).map(p => f -> p))

                                    // Best-effort exclusion refinement: subtract small MUST_NOT postings sets (and NotEquals when present).
                                    // IMPORTANT: exclusion refinement must be EXACT; otherwise we'd drop valid docs.
                                    // - `Equals` / `NotEquals`: only safe for non-string values (our postings normalize strings to lowercase).
                                    // - `StartsWith` / `EndsWith`: only safe when the query length <= prefixMaxLen (non-truncated).
                                    def refineExcludePrefixOpt(f: Filter[Doc]): Option[String] = f match {
                                      case e: Filter.Equals[Doc, _] =>
                                        val raw = TraversalIndex.valuesForIndexValue(e.value)
                                        raw match {
                                          case List(_: String) => None
                                          case List(_) =>
                                            refinePrefixOpt(f)
                                          case _ => None
                                        }
                                      case ne: Filter.NotEquals[Doc, _] =>
                                        val raw = TraversalIndex.valuesForIndexValue(ne.value)
                                        raw match {
                                          case List(_: String) => None
                                          case List(_) =>
                                            refinePrefixOpt(f)
                                          case _ => None
                                        }
                                      case s: Filter.StartsWith[Doc, _] =>
                                        Option(s.query).filter(_.nonEmpty).filter(_.length <= streamingPrefixMaxLen).flatMap(_ => refinePrefixOpt(f))
                                      case e: Filter.EndsWith[Doc, _] =>
                                        Option(e.query).filter(_.nonEmpty).filter(_.length <= streamingPrefixMaxLen).flatMap(_ => refinePrefixOpt(f))
                                      case _ => None
                                    }

                                    val excludeCandidates0 =
                                      if !streamingMultiRefineExcludeEnabled then Nil
                                      else mustNotClauses.flatMap(f => refineExcludePrefixOpt(f).map(p => f -> p))

                                    if includeCandidates0.isEmpty && excludeCandidates0.isEmpty then Task.pure(baseStream0)
                                    else {
                                      val includeLimited = includeCandidates0.take(streamingMultiRefineMaxClauses)
                                      val excludeLimited = excludeCandidates0.take(streamingMultiRefineMaxClauses)

                                      def smallSet(pfx: String): Task[Option[Set[String]]] =
                                        TraversalPersistedIndex.postingsTake(pfx, kv, streamingMultiRefineMaxIds + 1).map { ids =>
                                          if ids.size <= streamingMultiRefineMaxIds then Some(ids.toSet) else None
                                        }

                                      val includeSetsT: Task[List[Set[String]]] =
                                        includeLimited.map { case (_, pfx) => smallSet(pfx) }.tasks.map(_.flatten)
                                      val excludeSetsT: Task[List[Set[String]]] =
                                        excludeLimited.map { case (_, pfx) => smallSet(pfx) }.tasks.map(_.flatten)

                                      (for
                                        includeSets <- includeSetsT
                                        excludeSets <- excludeSetsT
                                      yield (includeSets, excludeSets)).map { case (includeSets, excludeSets) =>
                                        val afterInclude =
                                          if includeSets.isEmpty then baseStream0
                                          else includeSets.foldLeft(baseStream0) { (s, idSet) =>
                                            s.filter(doc => idSet.contains(doc._id.value))
                                          }
                                        val exclude =
                                          if excludeSets.isEmpty then Set.empty[String]
                                          else excludeSets.foldLeft(Set.empty[String])(_ union _)
                                        if exclude.isEmpty then afterInclude
                                        else afterInclude.filter(doc => !exclude.contains(doc._id.value))
                                      }
                                    }
                                  }
                                }
                              case None =>
                              // No MUST/FILTER driver. If we have SHOULD clauses, merge seedable SHOULD streams as a best-effort.
                              val seedableShould0 = shouldClauses.filter(isStreamingSeedable)
                              if seedableShould0.isEmpty then Task.pure(backing.stream)
                              else {
                                val shouldLimited = seedableShould0.take(streamingMultiMaxShouldDrivers)
                                // Order SHOULD drivers by estimated postings size (bounded), then by the same priority heuristic.
                                val orderedShouldT: Task[List[Filter[Doc]]] =
                                  shouldLimited
                                    .map(f => driverCountHint(f).map(c => f -> c))
                                    .tasks
                                    .map { pairs =>
                                      val (known, unknown) = pairs.partition(_._2.nonEmpty)
                                      val knownSorted = known.collect { case (f, Some(c)) => (f, c) }.sortBy { case (f, c) => (c, priority(f)) }.map(_._1)
                                      val unknownSorted = unknown.map(_._1).sortBy(priority)
                                      (knownSorted ++ unknownSorted).distinct
                                    }

                                orderedShouldT.flatMap { ordered =>
                                  if ordered.isEmpty then Task.pure(backing.stream)
                                  else {
                                    // If IndexOrder is requested, concatenating streams can break global ordering.
                                    // For page-only queries, use a bounded "take per clause + merge/sort" strategy.
                                    if query.sort == List(Sort.IndexOrder) && required > 0 && required <= streamingMaxInPageDocs then {
                                      def idsForClauseIndexOrder(f: Filter[Doc]): Task[Option[List[String]]] = f match {
                                        case e: Filter.Equals[Doc, _] =>
                                          if e.field(model).isTokenized then {
                                            val tokens = tokenizedTokensForEquals(e)
                                            if tokens.isEmpty then Task.pure(None)
                                            else {
                                              val prefixes = tokens.map(t => TraversalKeys.tokoPrefix(storeName, e.fieldName, t))
                                              prefixes
                                                .map { p =>
                                                  TraversalPersistedIndex.postingsTakeAndCountUpTo(p, kv, required, streamingMultiDriverMaxCount).map {
                                                    case (ids, c) => (ids, c)
                                                  }
                                                }
                                                .tasks
                                                .map { results =>
                                                  val best = results.minBy { case (ids, c) => (c, ids.size) }._1
                                                  Some(best)
                                                }
                                            }
                                          } else {
                                            val raw = TraversalIndex.valuesForIndexValue(e.value)
                                            val enc = raw.headOption.map {
                                              case null => "null"
                                              case s: String => s.toLowerCase
                                              case v => v.toString
                                            }.getOrElse("")
                                            TraversalPersistedIndex.postingsTake(TraversalKeys.eqoPrefix(storeName, e.fieldName, enc), kv, required).map(Some(_))
                                          }
                                        case s: Filter.StartsWith[Doc, _] =>
                                          val pOpt = Option(s.query).map(_.toLowerCase.take(streamingPrefixMaxLen)).filter(_.nonEmpty)
                                          pOpt match {
                                            case None => Task.pure(None)
                                            case Some(p) =>
                                              TraversalPersistedIndex.postingsTake(TraversalKeys.swoPrefix(storeName, s.fieldName, p), kv, required).map(Some(_))
                                          }
                                        case e: Filter.EndsWith[Doc, _] =>
                                          val pOpt = Option(e.query).map(_.toLowerCase.reverse.take(streamingPrefixMaxLen)).filter(_.nonEmpty)
                                          pOpt match {
                                            case None => Task.pure(None)
                                            case Some(p) =>
                                              TraversalPersistedIndex.postingsTake(TraversalKeys.ewoPrefix(storeName, e.fieldName, p), kv, required).map(Some(_))
                                          }
                                        case _ =>
                                          Task.pure(None)
                                      }

                                      ordered
                                        .map(idsForClauseIndexOrder)
                                        .tasks
                                        .flatMap { idOpts =>
                                          val lists = idOpts.flatten
                                          if lists.isEmpty then Task.pure(backing.stream)
                                          else {
                                            val ids0 = TraversalPersistedIndex.mergeSortedDistinctTake(lists, required)
                                            if ids0.isEmpty then Task.pure(rapid.Stream.empty)
                                            else if !streamingMultiRefineExcludeEnabled || mustNotClauses.isEmpty then {
                                              Task.pure(
                                                rapid.Stream
                                                  .emits(ids0)
                                                  .map(id => Id[Doc](id))
                                                  .evalMap(id => backing.get(id))
                                                  .collect { case Some(doc) => doc }
                                              )
                                            } else {
                                              // Bounded, correctness-safe MUST_NOT prefilter before fetching docs.
                                              def excludePrefixOpt(f: Filter[Doc]): Option[String] = f match {
                                                case e: Filter.Equals[Doc, _] =>
                                                  TraversalIndex.valuesForIndexValue(e.value) match {
                                                    case List(_: String) => None
                                                    case List(_) =>
                                                      val raw = TraversalIndex.valuesForIndexValue(e.value)
                                                      val enc = raw.headOption.map {
                                                        case null => "null"
                                                        case s: String => s.toLowerCase
                                                        case v => v.toString
                                                      }.getOrElse("")
                                                      Some(TraversalKeys.eqoPrefix(storeName, e.fieldName, enc))
                                                    case _ => None
                                                  }
                                                case ne: Filter.NotEquals[Doc, _] =>
                                                  TraversalIndex.valuesForIndexValue(ne.value) match {
                                                    case List(_: String) => None
                                                    case List(_) =>
                                                      val raw = TraversalIndex.valuesForIndexValue(ne.value)
                                                      val enc = raw.headOption.map {
                                                        case null => "null"
                                                        case s: String => s.toLowerCase
                                                        case v => v.toString
                                                      }.getOrElse("")
                                                      Some(TraversalKeys.eqoPrefix(storeName, ne.fieldName, enc))
                                                    case _ => None
                                                  }
                                                case s: Filter.StartsWith[Doc, _] =>
                                                  Option(s.query)
                                                    .filter(_.nonEmpty)
                                                    .filter(_.length <= streamingPrefixMaxLen)
                                                    .map(_.toLowerCase.take(streamingPrefixMaxLen))
                                                    .filter(_.nonEmpty)
                                                    .map(p => TraversalKeys.swoPrefix(storeName, s.fieldName, p))
                                                case e: Filter.EndsWith[Doc, _] =>
                                                  Option(e.query)
                                                    .filter(_.nonEmpty)
                                                    .filter(_.length <= streamingPrefixMaxLen)
                                                    .map(_.toLowerCase.reverse.take(streamingPrefixMaxLen))
                                                    .filter(_.nonEmpty)
                                                    .map(p => TraversalKeys.ewoPrefix(storeName, e.fieldName, p))
                                                case _ =>
                                                  None
                                              }

                                              val excludePrefixes =
                                                mustNotClauses.flatMap(excludePrefixOpt).distinct.take(streamingMultiRefineMaxClauses)
                                              if excludePrefixes.isEmpty then Task.pure(
                                                rapid.Stream
                                                  .emits(ids0)
                                                  .map(id => Id[Doc](id))
                                                  .evalMap(id => backing.get(id))
                                                  .collect { case Some(doc) => doc }
                                              )
                                              else {
                                                def smallSet(pfx: String): Task[Option[Set[String]]] =
                                                  TraversalPersistedIndex
                                                    .postingsTakeAndCountUpTo(pfx, kv, streamingMultiRefineMaxIds + 1, streamingMultiRefineMaxIds + 1)
                                                    .map { case (ids, c) => if c <= streamingMultiRefineMaxIds then Some(ids.toSet) else None }

                                                excludePrefixes.map(smallSet).tasks.map { sets =>
                                                  val exclude = sets.flatten.foldLeft(Set.empty[String])(_ union _)
                                                  val ids =
                                                    if exclude.isEmpty then ids0
                                                    else ids0.filterNot(exclude.contains)
                                                  rapid.Stream
                                                    .emits(ids)
                                                    .map(id => Id[Doc](id))
                                                    .evalMap(id => backing.get(id))
                                                    .collect { case Some(doc) => doc }
                                                }
                                              }
                                            }
                                          }
                                        }
                                    } else {
                                      val tasks: List[Task[rapid.Stream[Doc]]] = ordered.map(streamForClause)
                                      tasks.tasks.map { streams =>
                                        val seen = mutable.HashSet.empty[String]
                                        rapid.Stream.emits(streams).flatMap(identity).filter { d =>
                                          // de-dupe by docId to reduce downstream verify work
                                          seen.add(d._id.value)
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                        }
                      }
                    }
                  }

                (rapid.Stream.force(streamTask), true, false)
              case _ =>
                (backing.stream, false, false)
            }
        }

        // Fast-path: if the planner/seed resolved to an empty id set, we can return immediately without scanning.
        if seedIdsResolved.exists(_.isEmpty) then {
          Task.pure(
            SearchResults(
              model = model,
              offset = offset,
              limit = limit,
              total = if query.countTotal then Some(0) else None,
              streamWithScore = rapid.Stream.empty,
              facetResults = Map.empty,
              transaction = query.transaction
            )
          )
        } else {
        val canEarlyTerminate: Boolean =
          (!query.countTotal || fastTotal.nonEmpty) &&
            query.facets.isEmpty &&
            (sorts.isEmpty || sorts == List(Sort.IndexOrder) || alreadySorted) &&
            limitIntOpt.nonEmpty

        if canEarlyTerminate then {
          // Fast path: since we don't need totals/facets/sorting, we can stop once we have the requested page.
          val pageSizeInt = limitIntOpt.get

          val hitStream: rapid.Stream[(Doc, Double)] =
            docsStream.evalMap { doc =>
              Task {
                if !matches(doc) then None
                else {
                  val score = if needScore then bestMatchScore(query.filter, model, doc, state) else 0.0
                  val passesScore = query.minDocScore.forall(min => score >= min)
                  if passesScore then Some(doc -> score) else None
                }
              }
            }.collect { case Some(v) => v }

          val paged = hitStream.drop(offset).take(pageSizeInt)
          val streamWithScore = paged.map { case (doc, score) =>
            convert(doc, query.conversion, model, state, Map.empty) -> score
          }

          // If the seed stream is approximate + bounded (e.g. coarse range buckets, truncated prefix),
          // we might underfill the page. If that happens, fall back to a full scan early-termination.
          if !approxSeeded then {
            Task.pure(
              SearchResults(
                model = model,
                offset = offset,
                limit = limit,
                total = if query.countTotal then fastTotal else None,
                streamWithScore = streamWithScore,
                facetResults = Map.empty,
                transaction = query.transaction
              )
            )
          } else {
            val need = offset + pageSizeInt
            hitStream.take(need).toList.flatMap { seededHits =>
              if seededHits.size >= need then Task.pure(seededHits)
              else {
                // fallback to full scan for correctness
                val fullHitStream: rapid.Stream[(Doc, Double)] =
                  backing.stream.evalMap { doc =>
                    Task {
                      if !matches(doc) then None
                      else {
                        val score = if needScore then bestMatchScore(query.filter, model, doc, state) else 0.0
                        val passesScore = query.minDocScore.forall(min => score >= min)
                        if passesScore then Some(doc -> score) else None
                      }
                    }
                  }.collect { case Some(v) => v }

                fullHitStream.take(need).toList
              }
            }.map { hits =>
              val pageHits = hits.drop(offset).take(pageSizeInt)
              SearchResults(
                model = model,
                offset = offset,
                limit = limit,
                total = if query.countTotal then fastTotal else None,
                streamWithScore =
                  rapid.Stream.emits(pageHits).map { case (doc, score) =>
                    convert(doc, query.conversion, model, state, Map.empty) -> score
                  },
                facetResults = Map.empty,
                transaction = query.transaction
              )
            }
          }
        } else {
          // Facet accumulation across the full matched set (independent of paging).
          final case class FacetAccumulator(
            queryByField: Map[FacetField[Doc], FacetQuery[Doc]],
            countsByField: mutable.HashMap[FacetField[Doc], mutable.HashMap[String, Int]]
          ) {
            def add(doc: Doc): Unit = {
              queryByField.foreach { case (ff, fq) =>
                val values: List[FacetValue] = ff.get(doc, ff, state)

                if values.isEmpty then {
                  if ff.hierarchical && fq.path.isEmpty then bump(ff, "$ROOT$")
                } else {
                  values.foreach { fv =>
                    val path = fv.path
                    if ff.hierarchical then {
                      if path.startsWith(fq.path) then {
                        val child = if path.length == fq.path.length then "$ROOT$" else path(fq.path.length)
                        bump(ff, child)
                      }
                    } else {
                      val child = path.headOption.getOrElse("$ROOT$")
                      bump(ff, child)
                    }
                  }
                }
              }
            }

            private def bump(ff: FacetField[Doc], key: String): Unit = {
              val map = countsByField(ff)
              map.updateWith(key) {
                case Some(v) => Some(v + 1)
                case None => Some(1)
              }
            }

            def result(childrenLimitByField: Map[FacetField[Doc], Int]): Map[FacetField[Doc], FacetResult] =
              queryByField.map { case (ff, _) =>
                val counts = countsByField(ff).toMap
                val childrenLimit = childrenLimitByField.getOrElse(ff, Int.MaxValue)
                val sorted = counts.iterator
                  .filter(_._1 != "$ROOT$")
                  .toList
                  .sortBy { case (value, count) => (-count, value) }
                val top = sorted.take(childrenLimit).map { case (value, count) => FacetResultValue(value, count) }
                val totalCount = top.map(_.count).sum
                val childCount = counts.keySet.size
                ff -> FacetResult(top, childCount = childCount, totalCount = totalCount)
              }
          }

          val facetFields: List[FacetField[Doc]] = query.facets.map(_.field).distinct
          val facetAccOpt: Option[FacetAccumulator] =
            if facetFields.isEmpty then None
            else {
              val queryByField = query.facets.foldLeft(Map.empty[FacetField[Doc], FacetQuery[Doc]]) { case (m, fq) =>
                m.updated(fq.field, fq)
              }
              val counts = mutable.HashMap.empty[FacetField[Doc], mutable.HashMap[String, Int]]
              facetFields.foreach(ff => counts.put(ff, mutable.HashMap.empty))
              Some(FacetAccumulator(queryByField, counts))
            }

          val childrenLimitByField: Map[FacetField[Doc], Int] =
            query.facets.foldLeft(Map.empty[FacetField[Doc], Int]) { case (m, fq) =>
              m.updated(fq.field, fq.childrenLimit.getOrElse(Int.MaxValue))
            }

          val kForSortOpt: Option[Int] = if sorts.nonEmpty then limitIntOpt.map(l => offset + l) else None

          val pageBuffer = mutable.ArrayBuffer.empty[Hit]
          val pqOpt: Option[mutable.PriorityQueue[Hit]] =
            if sorts.nonEmpty && kForSortOpt.nonEmpty then {
              implicit val worstFirst: Ordering[Hit] = new Ordering[Hit] {
                override def compare(x: Hit, y: Hit): Int = compareHits(x, y) // larger = worse
              }
              Some(mutable.PriorityQueue.empty[Hit])
            } else None

          var matchedCount: Int = 0
          var totalCount: Int = 0

          def onMatch(doc: Doc, score: Double): Unit = {
            totalCount += 1
            facetAccOpt.foreach(_.add(doc))

            val hit = Hit(doc, score, keysFor(doc, score))

            pqOpt match {
              case Some(pq) =>
                pq.enqueue(hit)
                val k = kForSortOpt.get
                if pq.size > k then pq.dequeue()
              case None =>
                // No sorting (or unbounded sorting): keep only the requested page window when possible.
                val keep = limitIntOpt match {
                  case Some(l) => matchedCount >= offset && matchedCount < offset + l
                  case None => matchedCount >= offset
                }
                if keep then pageBuffer += hit
            }
          }

          // Stream consumption: single pass, no full materialization.
          val consume: Task[Int] = docsStream.evalMap { doc =>
            Task {
              if matches(doc) then {
                val score = if needScore then bestMatchScore(query.filter, model, doc, state) else 0.0
                val passesScore = query.minDocScore.forall(min => score >= min)
                if passesScore then {
                  onMatch(doc, score)
                  matchedCount += 1
                }
              }
            }
          }.count

          consume.map { _ =>
            val finalHits: Vector[Hit] =
              pqOpt match {
                case Some(pq) =>
                  val kept = pq.clone().dequeueAll.toVector // worst->best because pq is worst-first
                  val sorted = kept.sortWith((a, b) => compareHits(a, b) < 0)
                  limitIntOpt match {
                    case Some(l) => sorted.slice(offset, offset + l)
                    case None => sorted.drop(offset)
                  }
                case None =>
                  // already page-windowed
                  pageBuffer.toVector
              }

            val total: Option[Int] =
              if query.countTotal then {
                fastTotal.orElse(Some(totalCount))
              } else None
            val facetResults: Map[FacetField[Doc], FacetResult] =
              facetAccOpt.map(_.result(childrenLimitByField)).getOrElse(Map.empty)

            val streamWithScore = rapid.Stream.emits(finalHits.toList.map { h =>
              convert(h.doc, query.conversion, model, state, Map.empty) -> h.score
            })

            SearchResults(
              model = model,
              offset = offset,
              limit = limit,
              total = total,
              streamWithScore = streamWithScore,
              facetResults = facetResults,
              transaction = query.transaction
            )
          }
        }
        }
        }
      }
    }
    }
  }

  /**
   * Attempts to produce a selective seed set of document IDs from persisted traversal index entries.
   * Falls back to None when no usable seed can be derived.
   *
   * v1 strategy: use Equals / In / StartsWith seeds only, and verify remaining predicates via evalFilter.
   */
  private def seedCandidates[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    storeName: String,
    model: Model,
    filter: Option[Filter[Doc]],
    index: TraversalIndexCache[Doc, Model],
  ): Option[Set[String]] = {
    def equalsPostings(fieldName: String, value: Any): Set[String] = {
      index.equalsPostings(fieldName, value)
    }

    def priority(f: Filter[Doc]): Int = f match {
      case _: Filter.Equals[Doc, _] => 0
      case _: Filter.StartsWith[Doc, _] => 1
      case _: Filter.EndsWith[Doc, _] => 1
      case _: Filter.In[Doc, _] => 2
      case _: Filter.Contains[Doc, _] => 3
      case _: Filter.Regex[Doc, _] => 3
      case _: Filter.RangeLong[Doc] => 4
      case _: Filter.RangeDouble[Doc] => 4
      case _: Filter.Multi[Doc] => 5
      case _ => 6
    }

    def find(f: Filter[Doc]): Option[Set[String]] = f match {
      case _: Filter.MatchNone[Doc] =>
        // Fast path: no matches, avoid any scan/fetch.
        Some(Set.empty)
      case e: Filter.Equals[Doc, _] if e.field(model).indexed =>
        Some(equalsPostings(e.fieldName, e.value))
      case e: Filter.NotEquals[Doc, _] if e.field(model).indexed =>
        // Only meaningful for refinement (diff against an existing base). As a standalone seed, skip.
        None
      case i: Filter.In[Doc, _] if i.field(model).indexed =>
        val sets = i.values.asInstanceOf[Seq[Any]].map(v => equalsPostings(i.fieldName, v))
        Some(sets.foldLeft(Set.empty[String])(_ union _))
      case r: Filter.RangeLong[Doc] if r.field(model).indexed =>
        Some(index.rangeLongPostings(r.fieldName, r.from, r.to))
      case r: Filter.RangeDouble[Doc] if r.field(model).indexed =>
        Some(index.rangeDoublePostings(r.fieldName, r.from, r.to))
      case s: Filter.StartsWith[Doc, _] if s.field(model).indexed =>
        Some(index.startsWithPostings(s.fieldName, s.query))
      case c: Filter.Contains[Doc, _] if c.field(model).indexed =>
        // N-gram seeding is only meaningful for >= 3 chars. Shorter queries must scan+verify.
        val q = Option(c.query).getOrElse("").toLowerCase
        if q.length >= 3 then Some(index.containsPostings(c.fieldName, q)) else None
      case r: Filter.Regex[Doc, _] if r.field(model).indexed =>
        extractLiteralFromRegex(r.expression).flatMap { lit =>
          val s = lit.toLowerCase
          if s.length >= 3 then Some(index.containsPostings(r.fieldName, s)) else None
        }
      case m: Filter.Multi[Doc] =>
        val mustLike = m.filters.collect {
          case fc if fc.condition == Condition.Must || fc.condition == Condition.Filter => fc.filter
        }
        val should = m.filters.collect { case fc if fc.condition == Condition.Should => fc.filter }
        val mustNot = m.filters.collect { case fc if fc.condition == Condition.MustNot => fc.filter }

        def refineSeed(base: Set[String]): Set[String] = {
          // Best-effort refinement: intersect additional cheap MUST seeds and subtract cheap MUST_NOT seeds.
          // Only use cheap postings (priority <= 2) to keep overhead bounded.
          val (mustNotEquals, mustPos) = mustLike.partition {
            case _: Filter.NotEquals[Doc, _] => true
            case _ => false
          }
          val mustRefine = mustPos.filter(f => priority(f) <= 2).toList
          val notRefine = mustNot.filter(f => priority(f) <= 2).toList

          def excludeFor(f: Filter[Doc]): Option[Set[String]] = f match {
            case ne: Filter.NotEquals[Doc, _] if ne.field(model).indexed =>
              Some(equalsPostings(ne.fieldName, ne.value))
            case other =>
              find(other)
          }

          val afterMust = mustRefine.foldLeft(base) { (acc, f0) =>
            if acc.isEmpty then acc
            else find(f0).map(s => acc intersect s).getOrElse(acc)
          }
          val afterNotEquals = mustNotEquals.foldLeft(afterMust) { (acc, f0) =>
            if acc.isEmpty then acc
            else excludeFor(f0).map(s => acc diff s).getOrElse(acc)
          }
          // Apply MustNot exclusions.
          val afterNot = notRefine.foldLeft(afterNotEquals) { (acc, f0) =>
            if acc.isEmpty then acc
            else excludeFor(f0).map(s => acc diff s).getOrElse(acc)
          }

          // If minShould > 0 and all SHOULD clauses are seedable (cheap), apply thresholding to shrink candidates.
          val requiredShould = if should.nonEmpty then m.minShould else 0
          if requiredShould <= 0 || afterNot.isEmpty then afterNot
          else {
            val shouldRefine = should.filter(f => priority(f) <= 2).toList
            val seedsOpt: Option[List[Set[String]]] =
              shouldRefine.foldLeft(Option(List.empty[Set[String]])) { (acc, f0) =>
                acc.flatMap(list => find(f0).map(s => list :+ s))
              }
            seedsOpt match {
              case Some(seeds) if requiredShould > seeds.size =>
                Set.empty
              case Some(seeds) if requiredShould == 1 =>
                // Keep only docs that match at least one SHOULD.
                val union = seeds.foldLeft(Set.empty[String])(_ union _)
                afterNot intersect union
              case Some(seeds) if requiredShould == seeds.size =>
                afterNot intersect seeds.reduce(_ intersect _)
              case Some(seeds) =>
                val counts = mutable.HashMap.empty[String, Int]
                var overflow = false
                seeds.foreach { s =>
                  if !overflow then {
                    (afterNot intersect s).foreach { id =>
                      val next = counts.getOrElse(id, 0) + 1
                      counts.update(id, next)
                      if counts.size > MaxSeedSize then overflow = true
                    }
                  }
                }
                if overflow then afterNot
                else counts.iterator.collect { case (id, c) if c >= requiredShould => id }.toSet
              case None =>
                afterNot
            }
          }
        }

        // Prefer the smallest MUST/FILTER seed (best selectivity), but avoid expensive seed computations
        // when a cheap/selective seed exists.
        val mustSeedRaw: Option[Set[String]] = {
          val ordered = mustLike.sortBy(priority)
          @annotation.tailrec
          def loop(rest: List[Filter[Doc]], best: Option[Set[String]]): Option[Set[String]] = rest match {
            case Nil => best
            case h :: t =>
              find(h) match {
                case Some(s) if s.isEmpty =>
                  // Empty MUST seed means query is unsatisfiable; short-circuit to empty.
                  Some(Set.empty)
                case Some(s) =>
                  val nextBest = best match {
                    case Some(b) if b.size <= s.size => best
                    case _ => Some(s)
                  }
                  // If we found a single-hit (or very small) seed from a cheap predicate, stop early.
                  if nextBest.exists(_.size <= 1) && priority(h) <= 1 then nextBest
                  else loop(t, nextBest)
                case None =>
                  loop(t, best)
              }
          }
          loop(ordered.toList, None)
        }

        val mustSeed: Option[Set[String]] = mustSeedRaw.map(refineSeed)

        mustSeed.orElse {
          // Pure-SHOULD seeding is only safe when minShould >= 1 AND all SHOULD clauses are seedable.
          if mustLike.isEmpty && should.nonEmpty && m.minShould >= 1 then {
            val seedsOpt: Option[List[Set[String]]] =
              should.foldLeft(Option(List.empty[Set[String]])) { (acc, f0) =>
                acc.flatMap(list => find(f0).map(s => list :+ s))
              }

            seedsOpt.flatMap { seeds =>
              val req = m.minShould
              if req > seeds.size then Some(Set.empty) // impossible threshold
              else if req == seeds.size then Some(seeds.reduce(_ intersect _))
              else if req == 1 then {
                val union = seeds.foldLeft(Set.empty[String])(_ union _)
                if union.size > MaxSeedSize then None else Some(refineSeed(union))
              } else {
                // Threshold union: ids that appear in at least req SHOULD clauses.
                val counts = mutable.HashMap.empty[String, Int]
                val it = seeds.iterator
                var overflow = false
                while it.hasNext && !overflow do {
                  it.next().foreach { id =>
                    val next = counts.getOrElse(id, 0) + 1
                    counts.update(id, next)
                    if counts.size > MaxSeedSize then overflow = true
                  }
                }
                if overflow then None
                else Some(refineSeed(counts.iterator.collect { case (id, c) if c >= req => id }.toSet))
              }
            }
          } else None
        }
      case _ => None
    }

    // NOTE: empty seed sets are meaningful (unsatisfiable query) and should be preserved.
    filter.flatMap(find)
  }

  private def seedCandidatesPersisted[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    storeName: String,
    model: Model,
    filter: Option[Filter[Doc]],
    kv: lightdb.transaction.PrefixScanningTransaction[lightdb.KeyValue, lightdb.KeyValue.type]
  ): Task[Option[Set[String]]] = {
    // Bounded to avoid materializing huge candidate sets at scale.
    val maxSeedSize: Int = MaxSeedSize

    def priority(f: Filter[Doc]): Int = f match {
      case _: Filter.Equals[Doc, _] => 0
      case _: Filter.StartsWith[Doc, _] => 1
      case _: Filter.EndsWith[Doc, _] => 1
      case _: Filter.In[Doc, _] => 2
      case _: Filter.Contains[Doc, _] => 3
      case _: Filter.Regex[Doc, _] => 3
      case _: Filter.RangeLong[Doc] => 4
      case _: Filter.RangeDouble[Doc] => 4
      case _: Filter.Multi[Doc] => 5
      case _ => 6
    }

    def find(f: Filter[Doc]): Task[Option[Set[String]]] = f match {
      case _: Filter.MatchNone[Doc] =>
        // Fast path: no matches, avoid any scan/fetch.
        Task.pure(Some(Set.empty))
      case e: Filter.Equals[Doc, _] if e.field(model).indexed && e.field(model).isTokenized =>
        e.getJson(model) match {
          case fabric.Str(s, _) =>
            TraversalPersistedIndex.tokSeedPostings(storeName, e.fieldName, s, kv)
          case _ =>
            Task.pure(None)
        }
      case e: Filter.Equals[Doc, _] if e.field(model).indexed =>
        TraversalPersistedIndex.eqSeedPostings(storeName, e.fieldName, e.value, kv)
      case e: Filter.NotEquals[Doc, _] if e.field(model).indexed =>
        // Only meaningful for refinement (diff against an existing base). As a standalone seed, skip.
        Task.pure(None)
      case i: Filter.In[Doc, _] if i.field(model).indexed =>
        // Union of bounded equality seeds. If union would exceed maxSeedSize, treat as no-seed.
        i.values.asInstanceOf[Seq[Any]].toList
          .map(v => TraversalPersistedIndex.eqSeedPostings(storeName, i.fieldName, v, kv))
          .tasks
          .map { opts =>
            if opts.exists(_.isEmpty) then None
            else {
              val union = opts.flatten.foldLeft(Set.empty[String])(_ union _)
              if union.size > maxSeedSize then None else Some(union)
            }
          }
      case s: Filter.StartsWith[Doc, _] if s.field(model).indexed =>
        TraversalPersistedIndex.swSeedPostings(storeName, s.fieldName, s.query, kv)
      case e: Filter.EndsWith[Doc, _] if e.field(model).indexed =>
        TraversalPersistedIndex.ewSeedPostings(storeName, e.fieldName, e.query, kv)
      case r: Filter.RangeLong[Doc] if r.field(model).indexed =>
        TraversalPersistedIndex.rangeLongSeedPostings(storeName, r.fieldName, r.from, r.to, kv)
      case r: Filter.RangeDouble[Doc] if r.field(model).indexed =>
        TraversalPersistedIndex.rangeDoubleSeedPostings(storeName, r.fieldName, r.from, r.to, kv)
      case c: Filter.Contains[Doc, _] if c.field(model).indexed =>
        TraversalPersistedIndex.ngSeedPostings(storeName, c.fieldName, c.query, kv)
      case r: Filter.Regex[Doc, _] if r.field(model).indexed =>
        extractLiteralFromRegex(r.expression) match {
          case Some(lit) =>
            val s = lit.toLowerCase
            if s.length >= 3 then TraversalPersistedIndex.ngSeedPostings(storeName, r.fieldName, s, kv)
            else Task.pure(None)
          case None => Task.pure(None)
        }
      case m: Filter.Multi[Doc] =>
        val mustLike = m.filters.collect {
          case fc if fc.condition == Condition.Must || fc.condition == Condition.Filter => fc.filter
        }
        val should = m.filters.collect { case fc if fc.condition == Condition.Should => fc.filter }
        val mustNot = m.filters.collect { case fc if fc.condition == Condition.MustNot => fc.filter }

        def bestMustSeed: Task[Option[Set[String]]] = {
          val ordered = mustLike.sortBy(priority).toList
          def loop(rest: List[Filter[Doc]], best: Option[Set[String]]): Task[Option[Set[String]]] = rest match {
            case Nil => Task.pure(best)
            case h :: t =>
              find(h).flatMap {
                case Some(s) if s.isEmpty =>
                  // Empty MUST seed means query is unsatisfiable; short-circuit to empty.
                  Task.pure(Some(Set.empty))
                case Some(s) =>
                  val nextBest = best match {
                    case Some(b) if b.size <= s.size => best
                    case _ => Some(s)
                  }
                  if nextBest.exists(_.size <= 1) && priority(h) <= 1 then Task.pure(nextBest)
                  else loop(t, nextBest)
                case None =>
                  loop(t, best)
              }
          }
          loop(ordered, None)
        }

        def refineSeed(base: Set[String]): Task[Set[String]] = {
          // Refinement is best-effort: intersect additional cheap MUST seeds and subtract cheap MUST_NOT seeds.
          // Only use cheap postings (priority <= 2) to keep refinement overhead bounded.
          val (mustNotEquals, mustPos) = mustLike.partition {
            case _: Filter.NotEquals[Doc, _] => true
            case _ => false
          }
          val mustRefine = mustPos.filter(f => priority(f) <= 2).toList
          val notRefine = mustNot.filter(f => priority(f) <= 2).toList

          def excludeFor(f: Filter[Doc]): Task[Option[Set[String]]] = f match {
            case ne: Filter.NotEquals[Doc, _] if ne.field(model).indexed =>
              TraversalPersistedIndex.eqSeedPostings(storeName, ne.fieldName, ne.value, kv)
            case other =>
              find(other)
          }

          for
            mustSeeds <- mustRefine.map(find).tasks.map(_.flatten)
            notEqSeeds <- mustNotEquals.map(excludeFor).tasks.map(_.flatten)
            notSeeds <- notRefine.map(excludeFor).tasks.map(_.flatten)
            shouldSeedsOpt <-
              if should.nonEmpty && m.minShould > 0 then {
                val shouldRefine = should.filter(f => priority(f) <= 2).toList
                shouldRefine.map(find).tasks.map { opts =>
                  if opts.exists(_.isEmpty) then None else Some(opts.flatten)
                }
              } else Task.pure(None)
          yield {
            val afterMust = mustSeeds.foldLeft(base) { (acc, s) => if acc.isEmpty then acc else acc intersect s }
            val afterNotEquals = notEqSeeds.foldLeft(afterMust) { (acc, s) => if acc.isEmpty then acc else acc diff s }
            val afterNot = notSeeds.foldLeft(afterNotEquals) { (acc, s) => if acc.isEmpty then acc else acc diff s }
            val requiredShould = if should.nonEmpty then m.minShould else 0
            if requiredShould <= 0 || afterNot.isEmpty then afterNot
            else {
              shouldSeedsOpt match {
                case Some(seeds) if requiredShould > seeds.size =>
                  Set.empty
                case Some(seeds) if requiredShould == 1 =>
                  val union = seeds.foldLeft(Set.empty[String])(_ union _)
                  afterNot intersect union
                case Some(seeds) if requiredShould == seeds.size =>
                  afterNot intersect seeds.reduce(_ intersect _)
                case Some(seeds) =>
                  val counts = mutable.HashMap.empty[String, Int]
                  var overflow = false
                  seeds.foreach { s =>
                    if !overflow then {
                      (afterNot intersect s).foreach { id =>
                        val next = counts.getOrElse(id, 0) + 1
                        counts.update(id, next)
                        if counts.size > MaxSeedSize then overflow = true
                      }
                    }
                  }
                  if overflow then afterNot
                  else counts.iterator.collect { case (id, c) if c >= requiredShould => id }.toSet
                case None =>
                  afterNot
              }
            }
          }
        }

        bestMustSeed.flatMap {
          case some @ Some(_) =>
            // If we got a base seed, attempt cheap refinement (still correctness-safe because we only intersect/diff
            // using postings that represent true supersets/subsets; otherwise we skip).
            val base = some.get
            if base.isEmpty then Task.pure(some)
            else refineSeed(base).map(s => Some(s))
          case None =>
            if mustLike.isEmpty && should.nonEmpty && m.minShould >= 1 then {
              should.toList.map(find).tasks.map { opts =>
                // Only safe if ALL should clauses are seedable; otherwise seeding would drop matches.
                if opts.exists(_.isEmpty) then None
                else {
                  val seeds = opts.flatten
                  val req = m.minShould
                  if req > seeds.size then Some(Set.empty)
                  else if req == seeds.size then Some(seeds.reduce(_ intersect _))
                  else if req == 1 then {
                    val union = seeds.foldLeft(Set.empty[String])(_ union _)
                    if union.size > maxSeedSize then None else Some(union)
                  } else {
                    val counts = mutable.HashMap.empty[String, Int]
                    var overflow = false
                    seeds.foreach { s =>
                      if !overflow then {
                        s.foreach { id =>
                          val next = counts.getOrElse(id, 0) + 1
                          counts.update(id, next)
                          if counts.size > maxSeedSize then overflow = true
                        }
                      }
                    }
                    if overflow then None
                    else Some(counts.iterator.collect { case (id, c) if c >= req => id }.toSet)
                  }
                }
              }
            } else Task.pure(None)
        }
      case _ => Task.pure(None)
    }

    filter match {
      case Some(f) =>
        find(f)
      case None =>
        Task.pure(None)
    }
  }

  // Exposed for pipeline integration (DocPipeline.match). Returns docId strings.
  private[traversal] def seedCandidatesForPipeline[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    storeName: String,
    model: Model,
    filter: Option[Filter[Doc]],
    index: TraversalIndexCache[Doc, Model]
  ): Option[Set[String]] =
    seedCandidates(storeName, model, filter, index)

  private[traversal] def seedCandidatesForPipelinePersisted[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    storeName: String,
    model: Model,
    filter: Option[Filter[Doc]],
    kv: lightdb.transaction.PrefixScanningTransaction[lightdb.KeyValue, lightdb.KeyValue.type]
  ): Task[Option[Set[String]]] =
    TraversalPersistedIndex.isReady(storeName, kv).flatMap { ready =>
      if !ready then Task.pure(None)
      else seedCandidatesPersisted(storeName, model, filter, kv)
    }

  /**
   * Best-effort literal extraction for regex prefiltering.
   *
   * We only handle the simplest / safest cases:
   * - patterns with no obvious metacharacters (treat as literal)
   * - `.*literal.*` wrappers
   *
   * Anything more complex returns None (verify-only fallback).
   */
  private def extractLiteralFromRegex(expr: String): Option[String] = {
    if expr == null || expr.isEmpty then return None
    val s = expr
    val stripped =
      if s.startsWith(".*") && s.endsWith(".*") && s.length >= 4 then s.substring(2, s.length - 2)
      else s

    // Reject common regex metacharacters to avoid incorrect prefilters.
    val meta = Set('.', '*', '+', '?', '[', ']', '(', ')', '{', '}', '\\', '^', '$', '|')
    if stripped.exists(meta.contains) then None
    else Some(stripped)
  }

  private def convert[Doc <: Document[Doc], Model <: DocumentModel[Doc], V](
    doc: Doc,
    conversion: Conversion[Doc, V],
    model: Model,
    state: IndexingState,
    distanceCacheById: Map[String, List[Distance]]
  ): V = conversion match {
    case Conversion.Value(field) =>
      field.get(doc, field, state).asInstanceOf[V]
    case Conversion.Doc() => doc.asInstanceOf[V]
    case Conversion.Converted(c) => c(doc)
    case m: Conversion.Materialized[Doc @unchecked, Model @unchecked] =>
      val json = obj(m.fields.map(f => f.name -> f.getJson(doc, state)): _*)
      MaterializedIndex[Doc, Model](json, model).asInstanceOf[V]
    case Conversion.DocAndIndexes() =>
      val json = obj(model.indexedFields.map(f => f.name -> f.getJson(doc, state)): _*)
      MaterializedAndDoc[Doc, Model](json, model, doc).asInstanceOf[V]
    case j: Conversion.Json[Doc @unchecked] =>
      obj(j.fields.map(f => f.name -> f.getJson(doc, state)): _*).asInstanceOf[V]
    case d: Conversion.Distance[Doc, _] =>
      val distances = distanceCacheById.getOrElse(doc._id.value, {
        val list = d.field.get(doc, d.field, state) match {
          case null => Nil
          case l: List[_] => l.collect { case g: Geo => g }
          case s: Set[_] => s.toList.collect { case g: Geo => g }
          case g: Geo => List(g)
          case other => List(other).collect { case g: Geo => g }
        }
        list.map(g => Spatial.distance(g, d.from))
      })
      DistanceAndDoc(doc, distances).asInstanceOf[V]
  }

  private def sortResults[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    sorts: List[Sort],
    docs: Vector[(Doc, Double)],
    model: Model,
    state: IndexingState,
    distanceCacheById: Map[String, List[Distance]]
  ): Vector[(Doc, Double)] = {
    if sorts.isEmpty then return docs

    def dirFor(sort: Sort): SortDirection = sort match {
      case Sort.BestMatch(direction) => direction
      case Sort.ByField(_, direction) => direction
      case Sort.IndexOrder => SortDirection.Ascending
      case Sort.ByDistance(_, _, direction) => direction
    }

    def keyFor(sort: Sort, doc: Doc, score: Double): Any = sort match {
      case Sort.BestMatch(_) => score
      case Sort.IndexOrder => doc._id.value
      case Sort.ByField(field, _) =>
        val f = field.asInstanceOf[Field[Doc, Any]]
        f.get(doc, f, state)
      case Sort.ByDistance(_, _, _) =>
        val min = distanceCacheById
          .get(doc._id.value)
          .flatMap(_.map(_.valueInMeters).minOption)
          .getOrElse(Double.PositiveInfinity)
        min
    }

    docs.sortWith { case ((d1, s1), (d2, s2)) =>
      val cmp = sorts.iterator.map { sort =>
        val k1 = keyFor(sort, d1, s1)
        val k2 = keyFor(sort, d2, s2)
        val base = compareAny(k1, k2)
        if dirFor(sort) == SortDirection.Descending then -base else base
      }.find(_ != 0).getOrElse(0)

      if cmp != 0 then cmp < 0
      else d1._id.value < d2._id.value
    }
  }

  private def bestMatchScore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    filter: Option[Filter[Doc]],
    model: Model,
    doc: Doc,
    state: IndexingState
  ): Double = {
    // v0 heuristic: if we find a tokenized Equals filter, count token matches in that field.
    val maybe = firstTokenizedEquals(filter, model).map { case (fieldName, raw) =>
      val field = model.fieldByName[String](fieldName).asInstanceOf[Field[Doc, String]]
      val valueTokens =
        Option(field.get(doc, field, state))
          .getOrElse("")
          .toLowerCase
          .split("\\s+")
          .toList
          .map(_.trim)
          .filter(_.nonEmpty)
          .toSet
      val queryTokens =
        raw.toLowerCase
          .split("\\s+")
          .toList
          .map(_.trim)
          .filter(_.nonEmpty)
          .distinct
      queryTokens.count(valueTokens.contains).toDouble
    }
    maybe.getOrElse(0.0)
  }

  private def firstTokenizedEquals[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    filter: Option[Filter[Doc]],
    model: Model
  ): Option[(String, String)] = {
    def loop(f: Filter[Doc]): Option[(String, String)] = f match {
      case e: Filter.Equals[Doc, _] if e.value != null && e.value != None && e.field(model).isTokenized =>
        e.getJson(model) match {
          case fabric.Str(s, _) => Some(e.fieldName -> s)
          case _ => None
        }
      case m: Filter.Multi[Doc] =>
        m.filters.iterator.map(fc => loop(fc.filter)).collectFirst { case Some(v) => v }
      case _ => None
    }
    filter.flatMap(loop)
  }

  private[traversal] def evalFilter[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    filter: Filter[Doc],
    model: Model,
    doc: Doc,
    state: IndexingState
  ): Boolean = filter match {
    case _: Filter.MatchNone[Doc] => false
    case f: Filter.Equals[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      val v = field.get(doc, field, state)
      // Tokenized fields: equality is interpreted as matching all whitespace-separated tokens (AND semantics)
      // (matches default SQL tokenizedEqualsPart behavior).
      if field.isTokenized && f.value != null && f.value != None then {
        val raw: Option[String] = f.value match {
          case s: String => Some(s)
          case _ =>
            f.getJson(model) match {
              case fabric.Str(s, _) => Some(s)
              case _ => None
            }
        }
        val queryTokens =
          raw.toList
            .flatMap(_.toLowerCase.split("\\s+").toList.map(_.trim).filter(_.nonEmpty))
            .distinct
        if queryTokens.isEmpty then true
        else {
          val valueTokens =
            Option(v)
              .map(_.toString)
              .getOrElse("")
              .toLowerCase
              .split("\\s+")
              .toList
              .map(_.trim)
              .filter(_.nonEmpty)
              .toSet
          queryTokens.forall(valueTokens.contains)
        }
      } else
      iterable(v) match {
        case Some(values) =>
          iterable(f.value) match {
            case Some(req) => req.forall(values.toSet.contains)
            case None => values.toSet.contains(f.value)
          }
        case None =>
          v == f.value
      }
    case f: Filter.NotEquals[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      val v = field.get(doc, field, state)
      // Tokenized fields: not equals is interpreted as NOT(all tokens present) (matches SQL tokenizedNotEqualsPart).
      if field.isTokenized && f.value != null && f.value != None then {
        val raw: Option[String] = f.value match {
          case s: String => Some(s)
          case _ =>
            f.getJson(model) match {
              case fabric.Str(s, _) => Some(s)
              case _ => None
            }
        }
        val queryTokens =
          raw.toList
            .flatMap(_.toLowerCase.split("\\s+").toList.map(_.trim).filter(_.nonEmpty))
            .distinct
        if queryTokens.isEmpty then false
        else {
          val valueTokens =
            Option(v)
              .map(_.toString)
              .getOrElse("")
              .toLowerCase
              .split("\\s+")
              .toList
              .map(_.trim)
              .filter(_.nonEmpty)
              .toSet
          !queryTokens.forall(valueTokens.contains)
        }
      } else
      iterable(v) match {
        case Some(values) =>
          iterable(f.value) match {
            case Some(req) => !req.forall(values.toSet.contains)
            case None => !values.toSet.contains(f.value)
          }
        case None =>
          v != f.value
      }
    case f: Filter.In[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      val v = field.get(doc, field, state)
      iterable(v) match {
        case Some(values) =>
          val set: Set[Any] = f.values.asInstanceOf[Seq[Any]].toSet
          values.exists(set.contains)
        case None =>
          f.values.contains(v)
      }
    case f: Filter.RangeLong[Doc] =>
      val field = model.fieldByName[Any](f.fieldName).asInstanceOf[Field[Doc, Any]]
      val v = toLong(field.get(doc, field, state))
      v.exists { lv =>
        f.from.forall(lv >= _) && f.to.forall(lv <= _)
      }
    case f: Filter.RangeDouble[Doc] =>
      val field = model.fieldByName[Any](f.fieldName).asInstanceOf[Field[Doc, Any]]
      val v = toDouble(field.get(doc, field, state))
      v.exists { dv =>
        f.from.forall(dv >= _) && f.to.forall(dv <= _)
      }
    case f: Filter.StartsWith[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      val v = field.get(doc, field, state)
      iterable(v) match {
        case Some(values) => values.exists(v => Option(v).exists(_.toString.startsWith(f.query)))
        case None => Option(v).exists(_.toString.startsWith(f.query))
      }
    case f: Filter.EndsWith[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      val v = field.get(doc, field, state)
      iterable(v) match {
        case Some(values) => values.exists(v => Option(v).exists(_.toString.endsWith(f.query)))
        case None => Option(v).exists(_.toString.endsWith(f.query))
      }
    case f: Filter.Contains[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      val v = field.get(doc, field, state)
      iterable(v) match {
        case Some(values) => values.exists(v => Option(v).exists(_.toString.contains(f.query)))
        case None => Option(v).exists(_.toString.contains(f.query))
      }
    case f: Filter.Exact[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      val v = field.get(doc, field, state)
      iterable(v) match {
        case Some(values) => values.exists(v => Option(v).exists(_.toString == f.query))
        case None => Option(v).exists(_.toString == f.query)
      }
    case f: Filter.Regex[Doc, _] =>
      val field = f.field(model).asInstanceOf[Field[Doc, Any]]
      val v = field.get(doc, field, state)
      def matches(s: String): Boolean = Try(f.expression.r.findFirstIn(s).nonEmpty).getOrElse(false)
      iterable(v) match {
        case Some(values) => values.exists(v => matches(Option(v).map(_.toString).getOrElse("")))
        case None => matches(Option(v).map(_.toString).getOrElse(""))
      }
    case f: Filter.DrillDownFacetFilter[Doc] =>
      // Correctness-first: interpret facets as stored List[FacetValue] with "path" semantics.
      val field = model.fieldByName[List[lightdb.facet.FacetValue]](f.fieldName).asInstanceOf[Field[Doc, List[lightdb.facet.FacetValue]]]
      val values = field.get(doc, field, state)
      val path = f.path
      values.exists { fv =>
        if f.showOnlyThisLevel then {
          fv.path == path
        } else {
          fv.path.startsWith(path)
        }
      }
    case f: Filter.Distance[Doc] =>
      val field = model.fieldByName[Any](f.fieldName).asInstanceOf[Field[Doc, Any]]
      val v = field.get(doc, field, state)
      val geos: List[Geo] = iterable(v) match {
        case Some(values) => values.toList.collect { case g: Geo => g }
        case None =>
          v match {
            case g: Geo => List(g)
            case _ => Nil
          }
      }
      geos.exists(g => Spatial.distance(g, f.from).valueInMeters <= f.radius.valueInMeters)
    case f: Filter.ExistsChild[Doc @unchecked] =>
      // In a normal LightDB execution path, FilterPlanner.resolve will have already resolved ExistsChild.
      // If it reaches here, treat it as unsupported to avoid silent wrong answers.
      throw new UnsupportedOperationException("ExistsChild must be resolved before traversal execution")
    case m: Filter.Multi[Doc] =>
      val must = m.filters.filter(c => c.condition == Condition.Must || c.condition == Condition.Filter).map(_.filter)
      val mustNot = m.filters.filter(_.condition == Condition.MustNot).map(_.filter)
      val should = m.filters.filter(_.condition == Condition.Should).map(_.filter)
      val mustOk = must.forall(f => evalFilter(f, model, doc, state))
      val mustNotOk = mustNot.forall(f => !evalFilter(f, model, doc, state))
      val shouldCount = should.count(f => evalFilter(f, model, doc, state))
      val requiredShould = if should.nonEmpty then m.minShould else 0
      mustOk && mustNotOk && shouldCount >= requiredShould
  }

  private def iterable(value: Any): Option[Iterable[Any]] = value match {
    case null => None
    case it: Iterable[?] => Some(it.asInstanceOf[Iterable[Any]])
    case arr: Array[?] => Some(arr.toIndexedSeq.asInstanceOf[IndexedSeq[Any]])
    case jc: java.util.Collection[?] => Some(jc.toArray.toIndexedSeq.asInstanceOf[IndexedSeq[Any]])
    case _ => None
  }

  private def toLong(value: Any): Option[Long] = value match {
    case null => None
    case Some(v) => toLong(v)
    case None => None
    case t: lightdb.time.Timestamp => Some(t.value)
    case n: java.lang.Number => Some(n.longValue())
    case s: String => s.toLongOption
    case _ => None
  }

  private def toDouble(value: Any): Option[Double] = value match {
    case null => None
    case Some(v) => toDouble(v)
    case None => None
    case n: java.lang.Number => Some(n.doubleValue())
    case s: String => s.toDoubleOption
    case _ => None
  }

  private def computeFacets[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    facetQueries: List[FacetQuery[Doc]],
    docs: Seq[Doc],
    model: Model
  ): Map[FacetField[Doc], FacetResult] = {
    val facetFields: List[FacetField[Doc]] = facetQueries.map(_.field).distinct
    if facetFields.isEmpty then {
      Map.empty
    } else {
      // Last facet query per field wins (matches SearchResults map semantics).
      val queryByField: Map[FacetField[Doc], FacetQuery[Doc]] =
        facetQueries.foldLeft(Map.empty[FacetField[Doc], FacetQuery[Doc]]) { case (m, fq) =>
          m.updated(fq.field, fq)
        }

      val state = new IndexingState
      val countsByField = scala.collection.mutable.HashMap.empty[FacetField[Doc], scala.collection.mutable.HashMap[String, Int]]
      facetFields.foreach(ff => countsByField.put(ff, scala.collection.mutable.HashMap.empty))

      def bump(ff: FacetField[Doc], key: String): Unit = {
        val map = countsByField(ff)
        map.updateWith(key) {
          case Some(v) => Some(v + 1)
          case None => Some(1)
        }
      }

      docs.foreach { doc =>
        facetFields.foreach { ff =>
          val fq = queryByField(ff)
          val values: List[FacetValue] = ff.get(doc, ff, state)

          if values.isEmpty then {
            if ff.hierarchical && fq.path.isEmpty then bump(ff, "$ROOT$")
          } else {
            values.foreach { fv =>
              val path = fv.path
              if ff.hierarchical then {
                if path.startsWith(fq.path) then {
                  val child = if path.length == fq.path.length then "$ROOT$" else path(fq.path.length)
                  bump(ff, child)
                }
              } else {
                val child = path.headOption.getOrElse("$ROOT$")
                bump(ff, child)
              }
            }
          }
        }
      }

      queryByField.map { case (ff, fq) =>
        val counts = countsByField(ff).toMap
        val childrenLimit = fq.childrenLimit.getOrElse(Int.MaxValue)
        val sorted = counts.iterator
          .filter(_._1 != "$ROOT$")
          .toList
          .sortBy { case (value, count) => (-count, value) }
        val top = sorted.take(childrenLimit).map { case (value, count) => FacetResultValue(value, count) }
        val totalCount = top.map(_.count).sum
        val childCount = counts.keySet.size
        ff -> FacetResult(top, childCount = childCount, totalCount = totalCount)
      }
    }
  }
}


