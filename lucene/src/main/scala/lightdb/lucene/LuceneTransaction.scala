package lightdb.lucene

import fabric.define.DefType
import fabric.io.JsonFormatter
import fabric.rw.Asable
import fabric.{Arr, Json, Null, NumDec, NumInt, Str}
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.{FacetField, Tokenized}
import lightdb.field.{Field, IndexingState}
import lightdb.filter.{Filter, FilterPlanner, NestedQuerySupport, QueryOptimizer}
import lightdb.id.Id
import lightdb.materialized.MaterializedAggregate
import lightdb.spatial.{Geo, GeometryCollection, Line, MultiLine, MultiPoint, MultiPolygon, Point, Polygon}
import lightdb.store.Conversion
import lightdb.transaction.{CollectionTransaction, NestedQueryTransaction, RollbackSupport, Transaction}
import lightdb.util.Aggregator
import lightdb.{Query, SearchResults, Sort}
import lightdb.lucene.blockjoin.LuceneBlockJoinStore
import org.apache.lucene.document.{DoubleDocValuesField, DoubleField, IntField, LatLonDocValuesField, LatLonPoint, LatLonShape, LongField, NumericDocValuesField, SortedDocValuesField, StoredField, StringField, TextField, Document => LuceneDocument, Field => LuceneField}
import org.apache.lucene.facet.{FacetField => LuceneFacetField}
import org.apache.lucene.geo.{Line => LuceneLine, Polygon => LucenePolygon}
import org.apache.lucene.index.Term
import org.apache.lucene.search.{MatchAllDocsQuery, ScoreDoc}
import org.apache.lucene.util.BytesRef
import rapid.Task

case class LuceneTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: LuceneStore[Doc, Model],
  state: LuceneState[Doc],
  parent: Option[Transaction[Doc, Model]],
  writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model],
  ownedParent: Boolean = false
)
  extends CollectionTransaction[Doc, Model]
    with NestedQueryTransaction[Doc, Model]
    with RollbackSupport[Doc, Model] {
  override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)

  private lazy val searchBuilder = new LuceneSearchBuilder[Doc, Model](store, store.model, this)

  override def jsonStream: rapid.Stream[Json] =
    rapid.Stream.force(doSearch[Json](Query[Doc, Model, Json](this, Conversion.Json(store.fields))).map(_.stream))

  override protected def _get[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = {
    val filter = Filter.Equals(index, value)
    val query = Query[Doc, Model, Doc](this, Conversion.Doc(), filter = Some(filter), limit = Some(1))
    doSearch[Doc](query).flatMap(_.list).map(_.headOption)
  }

  override protected def _insert(doc: Doc): Task[Doc] = addDoc(doc, upsert = false)

  override protected def _upsert(doc: Doc): Task[Doc] = addDoc(doc, upsert = true)

  override protected def _exists(id: Id[Doc]): Task[Boolean] = get(id).map(_.nonEmpty)

  override protected def _count: Task[Int] = Task(state.indexSearcher.count(new MatchAllDocsQuery))

  override protected def _delete(id: Id[Doc]): Task[Boolean] = _deleteInternal(store.idField, id)

  protected def _deleteInternal[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Boolean] = Task {
    val query = searchBuilder.filter2Lucene(Some(index === value))
    store.index.indexWriter.deleteDocuments(query)
    true
  }

  override protected def _commit: Task[Unit] = state.commit

  override protected def _rollback: Task[Unit] = state.rollback

  override protected def _close: Task[Unit] = {
    val releaseParent = if ownedParent then {
      store.storeMode match {
        case lightdb.store.StoreMode.Indexes(storage) =>
          parent match {
            case Some(p) => storage.transaction.release(p.asInstanceOf[storage.TX]).unit
            case None => Task.unit
          }
        case _ => Task.unit
      }
    } else {
      Task.unit
    }

    releaseParent.next(state.close)
  }

  def groupBy[G, V](query: Query[Doc, Model, V],
                    groupField: Field[Doc, G],
                    docsPerGroup: Int = query.pageSize.getOrElse(1),
                    groupOffset: Int = query.offset,
                    groupLimit: Option[Int] = query.limit,
                    groupSort: List[Sort] = query.sort,
                    withinGroupSort: List[Sort] = Nil,
                    includeScores: Boolean = query.scoreDocs,
                    includeTotalGroupCount: Boolean = true): Task[LuceneGroupedSearchResults[Doc, Model, G, V]] = {
    val limit = groupLimit.getOrElse(100_000_000)
    val withinSort = if withinGroupSort.nonEmpty then withinGroupSort else groupSort
    searchBuilder.grouped(
      query = query,
      groupField = groupField,
      docsPerGroup = docsPerGroup,
      groupOffset = groupOffset,
      groupLimit = limit,
      groupSortList = groupSort,
      withinGroupSortList = withinSort,
      includeScores = includeScores,
      includeTotalGroupCount = includeTotalGroupCount
    )
  }

  private def doSearchDirect[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] =
    searchBuilder(query)

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] = {
    if NestedQuerySupport.containsNested(query.filter) then {
      doSearchWithNestedFallback(query)
    } else {
      doSearchDirect(query)
    }
  }

  private def doSearchWithNestedFallback[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] = {
    if query.facets.nonEmpty then {
      Task.error(new UnsupportedOperationException("Facets with nested fallback are not supported in Lucene"))
    } else if query.limit.isEmpty && query.pageSize.isEmpty && !query.countTotal then {
      Task.error(new UnsupportedOperationException(
        s"Lucene nested fallback requires limit or pageSize to keep memory bounded for store '${store.name}'."
      ))
    } else {
      NestedQuerySupport.validateFallbackCompatible(query.filter)
      val broadFilter = NestedQuerySupport.stripNested(query.filter)
      val broadQuery = Query[Doc, Model, Doc](this, Conversion.Doc()).copy(
        filter = broadFilter,
        sort = query.sort,
        offset = 0,
        limit = None,
        pageSize = None,
        countTotal = false,
        scoreDocs = query.scoreDocs || query.sort.exists(_.isInstanceOf[Sort.BestMatch]) || query.minDocScore.nonEmpty,
        minDocScore = query.minDocScore,
        facets = Nil,
        optimize = query.optimize
      )
      val requestedLimit = query.limit.orElse(query.pageSize).getOrElse(Int.MaxValue)
      val scanPageSize = math.max(1, math.min(query.pageSize.getOrElse(1000), requestedLimit))

      def loop(scanOffset: Int,
               matchedSoFar: Int,
               collected: Vector[(V, Double)]): Task[(Int, Vector[(V, Double)])] = {
        val enoughForPage = collected.size >= requestedLimit
        if !query.countTotal && enoughForPage then {
          Task.pure(matchedSoFar -> collected)
        } else {
          val pageQuery = broadQuery.copy(offset = scanOffset, limit = Some(scanPageSize), pageSize = None)
          doSearchDirect(pageQuery).flatMap(_.listWithScore).flatMap { page =>
            if page.isEmpty then {
              Task.pure(matchedSoFar -> collected)
            } else {
              var matched = matchedSoFar
              val buffer = scala.collection.mutable.ArrayBuffer.from(collected)
              page.foreach {
                case (doc, score) =>
                  if query.filter.forall(f => NestedQuerySupport.eval(f, store.model, doc)) then {
                    if matched >= query.offset && buffer.size < requestedLimit then {
                      buffer += (convertDoc(doc, query.conversion) -> score)
                    }
                    matched += 1
                  }
              }
              val continue =
                page.size >= scanPageSize &&
                  (query.countTotal || buffer.size < requestedLimit)
              if continue then loop(scanOffset + page.size, matched, buffer.toVector)
              else Task.pure(matched -> buffer.toVector)
            }
          }
        }
      }

      loop(scanOffset = 0, matchedSoFar = 0, collected = Vector.empty).map { case (matched, collected) =>
        SearchResults(
          model = store.model,
          offset = query.offset,
          limit = query.limit,
          total = if query.countTotal then Some(matched) else None,
          streamWithScore = rapid.Stream.emits(collected),
          facetResults = Map.empty,
          transaction = this
        )
      }
    }
  }

  /**
   * Prefer keyset (searchAfter) pagination for streaming on Lucene when possible.
   *
   * We only enable this when `offset == 0` (keyset pagination cannot jump to arbitrary offsets efficiently).
   * Otherwise we fall back to the default offset-based implementation.
   */
  override def streamScored[V](query: Query[Doc, Model, V]): rapid.Stream[(V, Double)] = {
    if query.pageSize.nonEmpty && query.offset == 0 && !NestedQuerySupport.containsNested(query.filter) then {
      val basePageSize = query.pageSize.getOrElse(1000)

      def loop(after: Option[ScoreDoc], remaining: Option[Int]): rapid.Stream[(V, Double)] =
        rapid.Stream.force {
          val reqSize = remaining match {
            case Some(r) if r <= 0 => 0
            case Some(r) => math.min(r, basePageSize)
            case None => basePageSize
          }
          if reqSize <= 0 then {
            Task.pure(rapid.Stream.empty)
          } else {
            searchBuilder.streamPageAfter(query = query, after = after, pageSize = reqSize).map {
              case (stream, nextAfter) =>
                val nextRemaining = remaining.map(_ - reqSize)
                nextAfter match {
                  case Some(na) if remaining.forall(_ > reqSize) =>
                    stream.append(loop(Some(na), nextRemaining))
                  case _ =>
                    stream
                }
            }
          }
        }

      loop(after = None, remaining = query.limit)
    } else {
      super.streamScored(query)
    }
  }

  override def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    Aggregator(query, store.model)

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] = aggregate(query).count

  override def distinct[F](query: Query[Doc, Model, _],
                           field: Field[Doc, F],
                           pageSize: Int): rapid.Stream[F] = {
    if pageSize <= 0 then {
      rapid.Stream.empty
    } else {
      // Lucene can do exact "distinct with filters" via grouping (term grouping on docvalues).
      //
      // We only support distinct on fields that have a corresponding docvalues sort field (`<name>Sort`), i.e.:
      // Str/Enum/Int/Dec and Option variants. (Arrays/objects don't have a single docvalues term.)
      def supported(defType: DefType): Boolean = defType match {
        case DefType.Str | DefType.Enum(_, _) | DefType.Int | DefType.Dec => true
        case DefType.Opt(d) => supported(d)
        case _ => false
      }
      if !supported(field.rw.definition) then {
        throw new NotImplementedError(s"distinct is not supported for field '${field.name}' (${field.rw.definition}) in Lucene")
      } else {
        rapid.Stream.force {
          val resolveExistsChild = !store.supportsNativeExistsChild
          FilterPlanner.resolve(query.filter, store.model, resolveExistsChild = resolveExistsChild).map { resolved =>
            val optimizedFilter = if query.optimize then resolved.map(QueryOptimizer.optimize) else resolved

            val baseQ: Query[Doc, Model, Id[Doc]] = Query(this, Conversion.Value(store.model._id))
              .copy(
                filter = optimizedFilter,
                sort = Nil,
                offset = 0,
                limit = None,
                pageSize = None,
                countTotal = false,
                scoreDocs = false,
                minDocScore = None,
                facets = Nil,
                optimize = false // already applied above
              )

            rapid.Stream(
              Task.defer {
                val lock = new AnyRef

                var groupOffset: Int = 0
                var emittedThisPage: Int = 0
                var currentPull: rapid.Pull[F] = rapid.Pull.fromList(Nil)
                var currentPullInitialized: Boolean = false
                var done: Boolean = false

                def closeCurrentPull(): Unit = {
                  try currentPull.close.handleError(_ => Task.unit).sync()
                  catch { case _: Throwable => () }
                }

                def fetchNextPull(): Unit = {
                  closeCurrentPull()
                  emittedThisPage = 0
                  val groups = groupBy(
                    query = baseQ,
                    groupField = field,
                    docsPerGroup = 1,
                    groupOffset = groupOffset,
                    groupLimit = Some(pageSize),
                    groupSort = Nil,
                    withinGroupSort = Nil,
                    includeScores = false,
                    includeTotalGroupCount = false
                  ).sync()

                  val values0 = groups.groups.iterator.map(_.group).toList
                  // Drop missing values (matches OpenSearch composite agg semantics).
                  val values = field.rw.definition match {
                    case DefType.Opt(_) => values0.filterNot(_ == None)
                    case _ => values0
                  }
                  currentPull = rapid.Pull.fromList(values)
                  currentPullInitialized = true
                }

                val pullTask: Task[rapid.Step[F]] = Task {
                  lock.synchronized {
                    @annotation.tailrec
                    def loop(): rapid.Step[F] = {
                      if done then {
                        rapid.Step.Stop
                      } else {
                        if !currentPullInitialized then {
                          fetchNextPull()
                        }

                        currentPull.pull.sync() match {
                          case e @ rapid.Step.Emit(_) =>
                            emittedThisPage += 1
                            e
                          case rapid.Step.Skip =>
                            loop()
                          case rapid.Step.Concat(inner) =>
                            currentPull = inner
                            loop()
                          case rapid.Step.Stop =>
                            closeCurrentPull()
                            if emittedThisPage < pageSize then {
                              done = true
                              rapid.Step.Stop
                            } else {
                              groupOffset += pageSize
                              currentPullInitialized = false
                              loop()
                            }
                        }
                      }
                    }
                    loop()
                  }
                }

                val closeTask: Task[Unit] = Task {
                  lock.synchronized {
                    done = true
                    closeCurrentPull()
                  }
                }

                Task.pure(rapid.Pull(pullTask, closeTask))
              }
            )
          }
        }
      }
    }
  }

  override def truncate: Task[Int] = for
    count <- this.count
    _ <- Task(store.index.indexWriter.deleteAll())
  yield count

  private def addDoc(doc: Doc, upsert: Boolean): Task[Doc] = Task {
    if store.isInstanceOf[LuceneBlockJoinStore[_, _, _, _]] then {
      throw new UnsupportedOperationException(
        s"${store.name} is a LuceneBlockJoinStore. Use block indexing (children first, parent last) instead of insert/upsert."
      )
    }
    if store.fields.tail.nonEmpty then {
      val id = doc._id
      val state = new IndexingState
      val luceneFields = store.fields.flatMap { field =>
        createLuceneFields(field, doc, state)
      }
      val document = new LuceneDocument
      luceneFields.foreach(document.add)

      if upsert then {
        store.index.indexWriter.updateDocument(new Term("_id", id.value), facetsPrepareDoc(document))
      } else {
        store.index.indexWriter.addDocument(facetsPrepareDoc(document))
      }
    }
    doc
  }

  private def convertDoc[V](doc: Doc, conversion: Conversion[Doc, V]): V = conversion match {
    case Conversion.Doc() =>
      doc.asInstanceOf[V]
    case Conversion.Value(field) =>
      field.get(doc, field, new IndexingState).asInstanceOf[V]
    case Conversion.Converted(c) =>
      c(doc)
    case Conversion.Materialized(fields) =>
      val state = new IndexingState
      val json = fabric.obj(fields.map(f => f.name -> f.getJson(doc, state)): _*)
      lightdb.materialized.MaterializedIndex[Doc, Model](json, store.model).asInstanceOf[V]
    case Conversion.DocAndIndexes() =>
      val state = new IndexingState
      val json = fabric.obj(store.fields.filter(_.indexed).map(f => f.name -> f.getJson(doc, state)): _*)
      lightdb.materialized.MaterializedAndDoc[Doc, Model](json, store.model, doc).asInstanceOf[V]
    case Conversion.Json(fields) =>
      val state = new IndexingState
      fabric.obj(fields.map(f => f.name -> f.getJson(doc, state)): _*).asInstanceOf[V]
    case Conversion.Distance(field, from, _, _) =>
      val state = new IndexingState
      val distance = field.get(doc, field, state).map(g => lightdb.spatial.Spatial.distance(from, g))
      lightdb.spatial.DistanceAndDoc(doc, distance).asInstanceOf[V]
  }

  private def createLuceneFields(field: Field[Doc, _], doc: Doc, state: IndexingState): List[LuceneField] = {
    def fs: LuceneField.Store = if store.storeMode.isAll || (field.stored && field.indexed) then LuceneField.Store.YES else LuceneField.Store.NO
    if fs == LuceneField.Store.NO then {
      scribe.info(s"*** ${store.name}.${field.name}")
    }
    val json = field.getJson(doc, state)
    var fields = List.empty[LuceneField]
    def add(field: LuceneField): Unit = fields = field :: fields
    field match {
      case ff: FacetField[Doc] => ff.get(doc, ff, state).flatMap { value =>
        if value.path.nonEmpty || ff.hierarchical then {
          val path = if ff.hierarchical then value.path ::: List("$ROOT$") else value.path
          Some(new LuceneFacetField(field.name, path: _*))
        } else {
          None
        }
      }
      case t: Tokenized[Doc] =>
        List(new LuceneField(field.name, t.get(doc, t, state), if fs == LuceneField.Store.YES then TextField.TYPE_STORED else TextField.TYPE_NOT_STORED))
      case _ =>
        def addJson(json: Json, d: DefType): Unit = {
          if field.isSpatial then {
            if json != Null then try {
              createGeoFields(field, json, add)
            } catch {
              case t: Throwable => throw new RuntimeException(s"Failure to populate geo field '${store.name}.${field.name}' for $doc (json: $json, className: ${field.className})", t)
            }
          } else {
            d match {
              case DefType.Str => json match {
                case Null => add(new StringField(field.name, Field.NullString, fs))
                case _ => add(new StringField(field.name, json.asString, fs))
              }
              case DefType.Enum(_, _) => add(new StringField(field.name, json.asString, fs))
              case DefType.Opt(d) => addJson(json, d)
              case DefType.Json | DefType.Obj(_, _) | DefType.Poly(_, _) => add(new StringField(field.name, JsonFormatter.Compact(json), fs))
              case _ if json == Null => // Ignore null values
              case DefType.Arr(d) =>
                val v = json.asVector
                if v.isEmpty then {
                  add(new StringField(field.name, "[]", fs))
                } else {
                  v.foreach(json => addJson(json, d))
                }
              case DefType.Bool => add(new IntField(field.name, if json.asBoolean then 1 else 0, fs))
              case DefType.Int => add(new LongField(field.name, json.asLong, fs))
              case DefType.Dec => add(new DoubleField(field.name, json.asDouble, fs))
              case _ => throw new UnsupportedOperationException(s"Unsupported definition (field: ${field.name}, className: ${field.className}): $d for $json")
            }
          }
        }
        addJson(json, field.rw.definition)

        val fieldSortName = s"${field.name}Sort"
        field.getJson(doc, state) match {
          case Str(s, _) =>
            val bytes = new BytesRef(s)
            add(new SortedDocValuesField(fieldSortName, bytes))
          case NumInt(l, _) =>
            add(new NumericDocValuesField(fieldSortName, l))
          case NumDec(d, _) =>
            val value = d.toDouble
            add(new DoubleDocValuesField(fieldSortName, value))
          case j if field.isSpatial && j != Null =>
            val list = j match {
              case Arr(values, _) => values.toList.map(_.as[Geo])
              case _ => List(j.as[Geo])
            }
            list.foreach { g =>
              add(new LatLonDocValuesField(fieldSortName, g.center.latitude, g.center.longitude))
            }
          case _ => // Ignore
        }
        fields
    }
  }

  private def createGeoFields(field: Field[Doc, _],
                              json: Json,
                              add: LuceneField => Unit): Unit = {
    field.className match {
      case _ =>
        def indexPoint(p: Point): Unit = try {
          LatLonShape.createIndexableFields(field.name, p.latitude, p.longitude)
        } catch {
          case t: Throwable => throw new RuntimeException(s"Failed to add LatLonPoint.createIndexableFields(${field.name}, ${p.latitude}, ${p.longitude}): ${JsonFormatter.Default(json)}", t)
        }
        def indexLine(l: Line): Unit = {
          val line = new LuceneLine(l.points.map(_.latitude).toArray, l.points.map(_.longitude).toArray)
          LatLonShape.createIndexableFields(field.name, line)
        }
        def indexPolygon(p: Polygon): Unit = {
          def convert(p: Polygon): LucenePolygon =
            new LucenePolygon(p.points.map(_.latitude).toArray, p.points.map(_.longitude).toArray)
          val polygon = convert(p)
          LatLonShape.createIndexableFields(field.name, polygon)
        }
        def indexGeo(geo: Geo): Unit = geo match {
          case p: Point => indexPoint(p)
          case MultiPoint(points) => points.foreach(indexPoint)
          case l: Line => indexLine(l)
          case MultiLine(lines) => lines.foreach(indexLine)
          case p: Polygon => indexPolygon(p)
          case MultiPolygon(polygons) => polygons.foreach(indexPolygon)
          case GeometryCollection(geometries) => geometries.foreach(indexGeo)
        }
        val list = json match {
          case Arr(value, _) => value.toList.map(_.as[Geo])
          case _ => List(json.as[Geo])
        }
        list.foreach { geo =>
          indexGeo(geo)
          add(new LatLonPoint(field.name, geo.center.latitude, geo.center.longitude))
        }
        if list.isEmpty then {
          add(new LatLonPoint(field.name, 0.0, 0.0))
        }
    }
    add(new StoredField(field.name, JsonFormatter.Compact(json)))
  }

  private def facetsPrepareDoc(doc: LuceneDocument): LuceneDocument = if store.hasFacets then {
    store.facetsConfig.build(store.index.taxonomyWriter, doc)
  } else {
    doc
  }
}
