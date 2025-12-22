package lightdb.traversal.store

import fabric.Json
import fabric.obj
import fabric.rw._
import lightdb.KeyValue
import lightdb.SortDirection
import lightdb.aggregate.{AggregateFilter, AggregateFunction, AggregateQuery, AggregateType}
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.field.Field.UniqueIndex
import lightdb.field.FieldAndValue
import lightdb.field.IndexingState
import lightdb.id.Id
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.{CollectionTransaction, PrefixScanningTransaction, Transaction}
import lightdb.{Query, SearchResults}
import rapid.Task
import rapid.taskSeq2Ops
import rapid.taskTaskOps

case class TraversalTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: TraversalStore[Doc, Model],
  parent: Option[Transaction[Doc, Model]]
) extends CollectionTransaction[Doc, Model] with PrefixScanningTransaction[Doc, Model] {
  private[store] var _backing: store.backing.TX = _
  def backing: store.backing.TX = _backing

  override def jsonStream: rapid.Stream[Json] = backing.jsonStream

  override def jsonPrefixStream(prefix: String): rapid.Stream[Json] = backing.jsonPrefixStream(prefix)

  override def truncate: Task[Int] = for {
    c <- backing.truncate
    _ <- Task(store.indexCache.clear())
    _ <- (
      if (store.persistedIndexEnabled && store.name != "_backingStore") {
        store.effectiveIndexBacking match {
          case Some(idx) =>
            idx.transaction { kv =>
              TraversalPersistedIndex.clearStore(store.name, kv.asInstanceOf[PrefixScanningTransaction[KeyValue, KeyValue.type]])
            }.attempt.unit
          case None => Task.unit
        }
      } else Task.unit
    )
  } yield c

  override protected def _get[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]] =
    backing.get(_ => index -> value)

  override protected def _insert(doc: Doc): Task[Doc] = backing.insert(doc).flatTap { d =>
    Task(store.indexCache.onInsert(d)).next {
      // best-effort persisted postings. Skip when we're already operating on _backingStore.
      if (store.persistedIndexEnabled && store.name != "_backingStore") {
        store.effectiveIndexBacking match {
          case Some(idx) =>
            idx.transaction { kv =>
              val tx = kv.asInstanceOf[PrefixScanningTransaction[KeyValue, KeyValue.type]]
              TraversalPersistedIndex
                .indexDoc(store.name, store.model, d, tx)
            }.attempt.map(_ => ())
          case None => Task.unit
        }
      } else Task.unit
    }
  }

  override def insert(docs: Seq[Doc]): Task[Seq[Doc]] = {
    if (!store.persistedIndexEnabled || store.name == "_backingStore") super.insert(docs)
    else {
      store.effectiveIndexBacking match {
        case None =>
          super.insert(docs)
        case Some(idx) =>
          // Bulk insert optimization: keep a single indexBacking transaction open and batch index writes per chunk.
          val ChunkSize: Int = 256
          idx.transaction { kv0 =>
            val kv = kv0.asInstanceOf[PrefixScanningTransaction[KeyValue, KeyValue.type]]
            rapid.Stream
              .emits(docs.toList)
              .chunk(ChunkSize)
              .evalMap { chunk =>
                val list = chunk.toList
                // Write docs to backing store first
                list.map { doc =>
                  store.trigger.insert(doc, this).flatMap(_ => backing.insert(doc)).flatTap { d =>
                    Task(store.indexCache.onInsert(d))
                  }
                }.tasks.flatMap { inserted =>
                  val postings = inserted.toList.flatMap(d => TraversalPersistedIndex.postingsForDoc(store.name, store.model, d))
                  if (postings.isEmpty) Task.pure(inserted)
                  else kv.upsert(postings).unit.map(_ => inserted)
                }
              }
              .toList
              .map(_.flatten)
          }.attempt.map(_.getOrElse(docs)) // best-effort index maintenance; docs are already inserted
      }
    }
  }

  override def upsert(docs: Seq[Doc]): Task[Seq[Doc]] = {
    if (!store.persistedIndexEnabled || store.name == "_backingStore") super.upsert(docs)
    else {
      store.effectiveIndexBacking match {
        case None =>
          super.upsert(docs)
        case Some(idx) =>
          val ChunkSize: Int = 256
          idx.transaction { kv0 =>
            val kv = kv0.asInstanceOf[PrefixScanningTransaction[KeyValue, KeyValue.type]]
            rapid.Stream
              .emits(docs.toList)
              .chunk(ChunkSize)
              .evalMap { chunk =>
                val list = chunk.toList
                val ids = list.map(_._id)

                // Fetch existing docs (best-effort); used for cache update + deindex.
                backing.getAll(ids).toList.flatMap { existingList =>
                  val existingMap: Map[Id[Doc], Doc] = existingList.iterator.map(d => d._id -> d).toMap

                  val upsertTasks: List[Task[Doc]] = list.map { doc =>
                    val existing = existingMap.get(doc._id)
                    store.trigger.upsert(doc, this).flatMap(_ => backing.upsert(doc)).flatTap { d =>
                      Task(store.indexCache.onUpsert(existing, d))
                    }
                  }

                  upsertTasks.tasks.flatMap { upserted =>
                    val deleteIds = existingList.flatMap(d => TraversalPersistedIndex.idsForDoc(store.name, store.model, d))
                    val deleteTask =
                      if (deleteIds.isEmpty) Task.unit else deleteIds.map(kv.delete).tasks.unit

                    val upsertPostings = upserted.toList.flatMap(d => TraversalPersistedIndex.postingsForDoc(store.name, store.model, d))
                    deleteTask.next {
                      if (upsertPostings.isEmpty) Task.pure(upserted)
                      else kv.upsert(upsertPostings).unit.map(_ => upserted)
                    }
                  }
                }
              }
              .toList
              .map(_.iterator.flatten.toList)
          }.attempt.map(_.getOrElse(docs)) // best-effort index maintenance
      }
    }
  }

  override protected def _upsert(doc: Doc): Task[Doc] = for {
    existing <- backing.get(doc._id)
    d <- backing.upsert(doc)
    _ <- Task(store.indexCache.onUpsert(existing, d))
    _ <- (
      if (store.persistedIndexEnabled && store.name != "_backingStore") {
        store.effectiveIndexBacking match {
          case Some(idx) =>
            idx.transaction { kv =>
              val tx = kv.asInstanceOf[PrefixScanningTransaction[KeyValue, KeyValue.type]]
              val de = existing match {
                case Some(old) => TraversalPersistedIndex.deindexDoc(store.name, store.model, old, tx)
                case None => Task.unit
              }
              de
                .next(TraversalPersistedIndex.indexDoc(store.name, store.model, d, tx))
            }.attempt.unit
          case None => Task.unit
        }
      } else Task.unit
    )
  } yield d

  override protected def _exists(id: Id[Doc]): Task[Boolean] = backing.exists(id)

  override protected def _count: Task[Int] = backing.count

  override protected def _delete[V](index: UniqueIndex[Doc, V], value: V): Task[Boolean] = for {
    existing <- backing.get(_ => index -> value)
    deleted <- backing.delete(_ => index -> value)
    _ <- Task(existing.foreach(store.indexCache.onDelete))
    _ <- (
      if (store.persistedIndexEnabled && store.name != "_backingStore") {
        (existing, store.effectiveIndexBacking) match {
          case (Some(old), Some(idx)) =>
            idx.transaction { kv =>
              val tx = kv.asInstanceOf[PrefixScanningTransaction[KeyValue, KeyValue.type]]
              TraversalPersistedIndex.deindexDoc(store.name, store.model, old, tx)
            }.attempt.unit
          case _ => Task.unit
        }
      } else Task.unit
    )
  } yield deleted

  override protected def _commit: Task[Unit] = backing.commit
  override protected def _rollback: Task[Unit] = backing.rollback
  override protected def _close: Task[Unit] = store.backing.transaction.release(backing)

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] =
    if (store.persistedIndexEnabled && store.name != "_backingStore") {
      store.effectiveIndexBacking match {
        case Some(idx) =>
          idx.transaction { kv =>
            val tx = kv.asInstanceOf[PrefixScanningTransaction[KeyValue, KeyValue.type]]
            TraversalQueryEngine.search(store.name, store.model, backing, store.indexCache, Some(tx), query)
          }
        case None =>
          TraversalQueryEngine.search(store.name, store.model, backing, store.indexCache, None, query)
      }
    } else {
      TraversalQueryEngine.search(store.name, store.model, backing, store.indexCache, None, query)
    }

  // Query.update/delete are provided by CollectionTransaction defaults; override not required initially.
  override def doUpdate[V](query: Query[Doc, Model, V], updates: List[FieldAndValue[Doc, _]]): Task[Int] =
    if (!store.persistedIndexEnabled || store.name == "_backingStore") super.doUpdate(query, updates)
    else {
      // Important: avoid streaming reads + writes against the same RocksDB transaction/iterator.
      // We materialize IDs first, then mutate in chunks.
      val ChunkSize: Int = 256
      query.id.stream.toList.flatMap { ids =>
        rapid.Stream
          .emits(ids)
          .chunk(ChunkSize)
          .evalMap { chunk =>
            val idChunk = chunk.toList
            if (idChunk.isEmpty) Task.pure(0)
            else {
              backing.getAll(idChunk).toList.flatMap { existingDocs =>
                val updatedDocs: List[Doc] = existingDocs.map { d =>
                  val json = store.model.rw.read(d)
                  val updatedJson = updates.foldLeft(json)((json, fv) => fv.update(json))
                  store.model.rw.write(updatedJson)
                }
                if (updatedDocs.isEmpty) Task.pure(0)
                else upsert(updatedDocs).map(_.size)
              }
            }
          }
          .toList
          .map(_.sum)
      }
    }

  override def doDelete[V](query: Query[Doc, Model, V]): Task[Int] =
    if (!store.persistedIndexEnabled || store.name == "_backingStore") super.doDelete(query)
    else {
      store.effectiveIndexBacking match {
        case None =>
          super.doDelete(query)
        case Some(idx) =>
          // Important: avoid streaming reads + writes against the same RocksDB transaction/iterator.
          // We materialize IDs first, then mutate in chunks.
          val ChunkSize: Int = 256
          query.id.stream.toList.flatMap { ids =>
            rapid.Stream
              .emits(ids)
              .chunk(ChunkSize)
              .evalMap { chunk =>
                val idChunk = chunk.toList
                if (idChunk.isEmpty) Task.pure(0)
                else {
                  idx.transaction { kv0 =>
                    val kv = kv0.asInstanceOf[PrefixScanningTransaction[KeyValue, KeyValue.type]]
                    for {
                      existingList <- backing.getAll(idChunk).toList
                      deleteIds = existingList.flatMap(d => TraversalPersistedIndex.idsForDoc(store.name, store.model, d))
                      _ <- if (deleteIds.isEmpty) Task.unit else deleteIds.map(kv.delete).tasks.unit
                      bools <- idChunk.map { id =>
                        store.trigger.delete(store.idField, id, this).flatMap(_ => backing.delete(_ => backing.store.idField -> id))
                      }.tasks
                      _ <- Task(existingList.foreach(store.indexCache.onDelete))
                    } yield bools.count(_ == true)
                  }
                }
              }
              .toList
              .map(_.sum)
          }
      }
    }

  override def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    rapid.Stream.force {
      val groupFunctions: List[AggregateFunction[_, _, Doc]] =
        query.functions.filter(_.`type` == AggregateType.Group)

      def unwrap(v: Any): Any = v match {
        case null => null
        case Some(x) => unwrap(x)
        case None => null
        case other => other
      }

      def toLong(v: Any): Option[Long] = unwrap(v) match {
        case null => None
        case t: lightdb.time.Timestamp => Some(t.value)
        case n: java.lang.Number => Some(n.longValue())
        case s: String => s.toLongOption
        case _ => None
      }
      def toDouble(v: Any): Option[Double] = unwrap(v) match {
        case null => None
        case n: java.lang.Number => Some(n.doubleValue())
        case s: String => s.toDoubleOption
        case _ => None
      }

      def evalAggFilter(row: Map[String, Any], f: AggregateFilter[Doc]): Boolean = {
        def v(name: String): Any = row.getOrElse(name, null)
        f match {
          case af: AggregateFilter.Equals[Doc, _] =>
            TraversalQueryEngine.compareAny(v(af.name), af.value) == 0
          case af: AggregateFilter.NotEquals[Doc, _] =>
            TraversalQueryEngine.compareAny(v(af.name), af.value) != 0
          case af: AggregateFilter.In[Doc, _] =>
            af.values.asInstanceOf[Seq[Any]].exists(x => TraversalQueryEngine.compareAny(v(af.name), x) == 0)
          case af: AggregateFilter.Regex[Doc, _] =>
            val s = Option(v(af.name)).map(_.toString).getOrElse("")
            try af.expression.r.findFirstIn(s).nonEmpty
            catch { case _: Throwable => false }
          case af: AggregateFilter.Combined[Doc] =>
            af.filters.forall(evalAggFilter(row, _))
          case af: AggregateFilter.RangeLong[Doc] =>
            toLong(v(af.name)).exists { lv =>
              af.from.forall(lv >= _) && af.to.forall(lv <= _)
            }
          case af: AggregateFilter.RangeDouble[Doc] =>
            toDouble(v(af.name)).exists { dv =>
              af.from.forall(dv >= _) && af.to.forall(dv <= _)
            }
          case AggregateFilter.StartsWith(name, _, q) =>
            Option(v(name)).exists(_.toString.startsWith(q))
          case AggregateFilter.EndsWith(name, _, q) =>
            Option(v(name)).exists(_.toString.endsWith(q))
          case AggregateFilter.Contains(name, _, q) =>
            Option(v(name)).exists(_.toString.contains(q))
          case AggregateFilter.Exact(name, _, q) =>
            Option(v(name)).exists(_.toString == q)
          case _: AggregateFilter.Distance[Doc] =>
            throw new UnsupportedOperationException("Distance not supported in traversal aggregates")
        }
      }

      // Grouped aggregation (multiple rows).
      if (groupFunctions.nonEmpty) {
        val state = new IndexingState

        final case class AggSpec(
          name: String,
          field: Field[Doc, Any],
          `type`: AggregateType,
          rw: RW[Any]
        )

        val groupSpecs: List[AggSpec] = groupFunctions.map { f0 =>
          val f = f0.asInstanceOf[AggregateFunction[Any, Any, Doc]]
          AggSpec(f.name, f.field.asInstanceOf[Field[Doc, Any]], f.`type`, f.tRW.asInstanceOf[RW[Any]])
        }

        val allSpecs: List[AggSpec] = query.functions.map { f0 =>
          val f = f0.asInstanceOf[AggregateFunction[Any, Any, Doc]]
          AggSpec(f.name, f.field.asInstanceOf[Field[Doc, Any]], f.`type`, f.tRW.asInstanceOf[RW[Any]])
        }

        val nonGroupSpecs: List[AggSpec] = allSpecs.filterNot(_.`type` == AggregateType.Group)

        // Per-group accumulator state.
        final case class GroupState(
          // For Avg: store (sum, count); for Sum: (hasFractional, sumL, sumD, sawAny); for Min/Max: cur; for Count: count;
          // for CountDistinct: set; for Concat: ArrayBuffer[(docId,value)] and optional seen set.
          values: scala.collection.mutable.HashMap[String, Any]
        )

        def initGroupState(): GroupState = GroupState(scala.collection.mutable.HashMap.empty)

        def updateGroup(gs: GroupState, docId: String, doc: Doc): Unit = {
          nonGroupSpecs.foreach { spec =>
            val current = gs.values.getOrElse(spec.name, null)
            val raw = spec.field.get(doc, spec.field, state)
            spec.`type` match {
              case AggregateType.Min =>
                val v = raw
                if (v != null) {
                  val next =
                    if (current == null) v
                    else if (TraversalQueryEngine.compareAny(v, current) < 0) v
                    else current
                  gs.values.update(spec.name, next)
                }
              case AggregateType.Max =>
                val v = raw
                if (v != null) {
                  val next =
                    if (current == null) v
                    else if (TraversalQueryEngine.compareAny(v, current) > 0) v
                    else current
                  gs.values.update(spec.name, next)
                }
              case AggregateType.Avg =>
                raw match {
                  case n: java.lang.Number =>
                    val (sum, count) = current match {
                      case t: (Double, Long) @unchecked => t
                      case _ => (0.0, 0L)
                    }
                    gs.values.update(spec.name, (sum + n.doubleValue(), count + 1L))
                  case _ => // ignore
                }
              case AggregateType.Sum =>
                raw match {
                  case n: java.lang.Number =>
                    val (hasFrac, sumL, sumD, sawAny) = current match {
                      case t: (Boolean, Long, Double, Boolean) @unchecked => t
                      case _ => (false, 0L, 0.0, false)
                    }
                    val d = n.doubleValue()
                    def isWholeLong(dd: Double): Boolean =
                      dd.isFinite && dd >= Long.MinValue.toDouble && dd <= Long.MaxValue.toDouble && dd.toLong.toDouble == dd
                    if (!hasFrac && isWholeLong(d)) {
                      gs.values.update(spec.name, (false, sumL + n.longValue(), sumD, true))
                    } else {
                      val baseD = if (hasFrac) sumD else sumL.toDouble
                      gs.values.update(spec.name, (true, sumL, baseD + d, true))
                    }
                  case _ => // ignore
                }
              case AggregateType.Count =>
                if (raw != null) {
                  val c = current match {
                    case i: Int => i
                    case _ => 0
                  }
                  gs.values.update(spec.name, c + 1)
                }
              case AggregateType.CountDistinct =>
                if (raw != null) {
                  val set = current match {
                    case s: scala.collection.mutable.HashSet[Any] @unchecked => s
                    case _ => scala.collection.mutable.HashSet.empty[Any]
                  }
                  set += raw
                  gs.values.update(spec.name, set)
                }
              case AggregateType.Concat =>
                if (raw != null) {
                  val buf = current match {
                    case b: scala.collection.mutable.ArrayBuffer[(String, Any)] @unchecked => b
                    case _ => scala.collection.mutable.ArrayBuffer.empty[(String, Any)]
                  }
                  buf += (docId -> raw)
                  gs.values.update(spec.name, buf)
                }
              case AggregateType.ConcatDistinct =>
                if (raw != null) {
                  val pair = current match {
                    case p: (scala.collection.mutable.ArrayBuffer[(String, Any)], scala.collection.mutable.HashSet[Any]) @unchecked => p
                    case _ => (scala.collection.mutable.ArrayBuffer.empty[(String, Any)], scala.collection.mutable.HashSet.empty[Any])
                  }
                  val (buf, seen) = pair
                  if (seen.add(raw)) buf += (docId -> raw)
                  gs.values.update(spec.name, (buf, seen))
                }
              case AggregateType.Group =>
                () // handled by group key
            }
          }
        }

        val groups = scala.collection.mutable.HashMap.empty[List[Any], GroupState]

        val processed: Task[Unit] =
          query.query.docs.stream.evalMap { doc =>
            Task {
              val docId = doc._id.value
              val key: List[Any] = groupSpecs.map(gs => gs.field.get(doc, gs.field, state))
              val st = groups.getOrElseUpdate(key, initGroupState())
              updateGroup(st, docId, doc)
            }
          }.drain

        processed.map { _ =>
          // Materialize all group rows.
          var rows: List[Map[String, Any]] = groups.iterator.map { case (key, st) =>
            val base: scala.collection.mutable.HashMap[String, Any] = scala.collection.mutable.HashMap.empty

            // Add group fields (both alias and raw field.name for Sort.ByField compatibility).
            groupSpecs.zip(key).foreach { case (gs, v0) =>
              base.update(gs.name, v0)
              if (!base.contains(gs.field.name)) base.update(gs.field.name, v0)
            }

            // Finalize non-group values.
            nonGroupSpecs.foreach { spec =>
              val cur = st.values.getOrElse(spec.name, null)
              val out: Any = spec.`type` match {
                case AggregateType.Avg =>
                  cur match {
                    case (sum: Double, count: Long) => if (count == 0L) 0.0 else sum / count.toDouble
                    case _ => 0.0
                  }
                case AggregateType.Sum =>
                  cur match {
                    case (hasFrac: Boolean, sumL: Long, sumD: Double, sawAny: Boolean) =>
                      if (!sawAny) 0
                      else if (hasFrac) sumD
                      else if (sumL.isValidInt) sumL.toInt
                      else sumL
                    case _ => 0
                  }
                case AggregateType.CountDistinct =>
                  cur match {
                    case set: scala.collection.mutable.HashSet[Any] @unchecked => set.size
                    case _ => 0
                  }
                case AggregateType.Concat =>
                  cur match {
                    case buf: scala.collection.mutable.ArrayBuffer[(String, Any)] @unchecked =>
                      buf.sortBy(_._1).map(_._2).toList
                    case _ => Nil
                  }
                case AggregateType.ConcatDistinct =>
                  cur match {
                    case (buf: scala.collection.mutable.ArrayBuffer[(String, Any)], _) =>
                      buf.sortBy(_._1).map(_._2).toList
                    case _ => Nil
                  }
                case _ =>
                  // Min/Max/Count already final.
                  cur
              }
              base.update(spec.name, out)
            }

            base.toMap
          }.toList

          // HAVING
          query.filter.foreach { f =>
            rows = rows.filter(r => evalAggFilter(r, f))
          }

          // Sorting: support AggregateQuery.sort plus underlying Query.sort like SQL does.
          val sortKeys: List[(String, SortDirection)] =
            (query.sort ::: query.query.sort).map {
              case (af: AggregateFunction[?, ?, Doc] @unchecked, dir: SortDirection) => af.name -> dir
              case lightdb.Sort.ByField(field, dir) => field.name -> dir
              case other => throw new UnsupportedOperationException(s"Unsupported aggregate sort: $other")
            }

          if (sortKeys.nonEmpty) {
            rows = rows.sortWith { (a, b) =>
              val cmp = sortKeys.iterator.map { case (k, dir) =>
                val base = TraversalQueryEngine.compareAny(a.getOrElse(k, null), b.getOrElse(k, null))
                if (dir == SortDirection.Descending) -base else base
              }.find(_ != 0).getOrElse(0)
              if (cmp != 0) cmp < 0
              else a.toString < b.toString
            }
          } else {
            // Deterministic default: order by group key string form.
            rows = rows.sortBy(_.get(groupSpecs.head.name).map(_.toString).getOrElse(""))
          }

          // Pagination from underlying query.
          val offset = query.query.offset
          val limit = query.query.limit.orElse(query.query.pageSize)
          val paged = limit match {
            case Some(l) => rows.slice(offset, offset + l)
            case None => rows.drop(offset)
          }

          val iterator = paged.iterator.map { row =>
            val json = obj(allSpecs.map { spec =>
              val v = row.getOrElse(spec.name, null)
              spec.name -> spec.rw.read(v)
            }: _*)
            MaterializedAggregate[Doc, Model](json, store.model)
          }

          rapid.Stream.fromIterator(Task(iterator))
        }
      } else {
      // Streaming-first aggregate execution:
      // - Single-pass reductions for min/max/sum/avg/count/countDistinct
      // - concat/concatDistinct inherently produce a full list result, so we still materialize their values, but we
      //   materialize only the concatenated values (not all docs).
      val state = new IndexingState

      sealed trait AggComputer {
        def name: String
        def rw: RW[Any]
        def update(docId: String, doc: Doc): Unit
        def value(): Any
      }

      final case class MinAgg(name: String, rw: RW[Any], field: Field[Doc, Any]) extends AggComputer {
        private var cur: Any = null
        override def update(docId: String, doc: Doc): Unit = {
          val v = field.get(doc, field, state)
          if (v != null) {
            if (cur == null) cur = v
            else if (TraversalQueryEngine.compareAny(v, cur) < 0) cur = v
          }
        }
        override def value(): Any = cur
      }

      final case class MaxAgg(name: String, rw: RW[Any], field: Field[Doc, Any]) extends AggComputer {
        private var cur: Any = null
        override def update(docId: String, doc: Doc): Unit = {
          val v = field.get(doc, field, state)
          if (v != null) {
            if (cur == null) cur = v
            else if (TraversalQueryEngine.compareAny(v, cur) > 0) cur = v
          }
        }
        override def value(): Any = cur
      }

      final case class AvgAgg(name: String, rw: RW[Any], field: Field[Doc, Any]) extends AggComputer {
        private var sum: Double = 0.0
        private var count: Long = 0L
        override def update(docId: String, doc: Doc): Unit = {
          field.get(doc, field, state) match {
            case n: java.lang.Number =>
              sum += n.doubleValue()
              count += 1L
            case _ => // ignore
          }
        }
        override def value(): Any = if (count == 0L) 0.0 else sum / count.toDouble
      }

      final case class SumAgg(name: String, rw: RW[Any], field: Field[Doc, Any]) extends AggComputer {
        private var hasFractional: Boolean = false
        private var sumL: Long = 0L
        private var sumD: Double = 0.0
        private var sawAny: Boolean = false

        private def isWholeLong(d: Double): Boolean = {
          d.isFinite &&
            d >= Long.MinValue.toDouble &&
            d <= Long.MaxValue.toDouble &&
            d.toLong.toDouble == d
        }

        override def update(docId: String, doc: Doc): Unit = {
          field.get(doc, field, state) match {
            case n: java.lang.Number =>
              sawAny = true
              val d = n.doubleValue()
              if (!hasFractional && isWholeLong(d)) {
                sumL += n.longValue()
              } else {
                if (!hasFractional) {
                  hasFractional = true
                  sumD = sumL.toDouble
                }
                sumD += d
              }
            case _ => // ignore
          }
        }

        override def value(): Any = {
          if (!sawAny) 0
          else if (hasFractional) sumD
          else if (sumL.isValidInt) sumL.toInt
          else sumL
        }
      }

      final case class CountAgg(name: String, rw: RW[Any], field: Field[Doc, Any]) extends AggComputer {
        private var count: Int = 0
        override def update(docId: String, doc: Doc): Unit = {
          val v = field.get(doc, field, state)
          if (v != null) count += 1
        }
        override def value(): Any = count
      }

      final case class CountDistinctAgg(name: String, rw: RW[Any], field: Field[Doc, Any]) extends AggComputer {
        private val set = scala.collection.mutable.HashSet.empty[Any]
        override def update(docId: String, doc: Doc): Unit = {
          val v = field.get(doc, field, state)
          if (v != null) set += v
        }
        override def value(): Any = set.size
      }

      final case class ConcatAgg(name: String, rw: RW[Any], field: Field[Doc, Any], distinct: Boolean) extends AggComputer {
        private val buf = scala.collection.mutable.ArrayBuffer.empty[(String, Any)]
        private val seen = if (distinct) Some(scala.collection.mutable.HashSet.empty[Any]) else None

        override def update(docId: String, doc: Doc): Unit = {
          val v = field.get(doc, field, state)
          if (v != null) {
            if (seen.forall(s => s.add(v))) {
              buf += (docId -> v)
            }
          }
        }

        override def value(): Any = {
          // Keep output deterministic: sort by docId (matches previous behavior which sorted docs by _id).
          buf.sortBy(_._1).map(_._2).toList
        }
      }

      // Build computers once.
      val computers: List[AggComputer] = query.functions.map { f0 =>
        val f = f0.asInstanceOf[lightdb.aggregate.AggregateFunction[Any, Any, Doc]]
        val field = f.field.asInstanceOf[Field[Doc, Any]]
        val rw = f0.asInstanceOf[lightdb.aggregate.AggregateFunction[Any, Any, Doc]].tRW.asInstanceOf[RW[Any]]
        f.`type` match {
          case lightdb.aggregate.AggregateType.Min => MinAgg(f.name, rw, field)
          case lightdb.aggregate.AggregateType.Max => MaxAgg(f.name, rw, field)
          case lightdb.aggregate.AggregateType.Avg => AvgAgg(f.name, rw, field)
          case lightdb.aggregate.AggregateType.Sum => SumAgg(f.name, rw, field)
          case lightdb.aggregate.AggregateType.Count => CountAgg(f.name, rw, field)
          case lightdb.aggregate.AggregateType.CountDistinct => CountDistinctAgg(f.name, rw, field)
          case lightdb.aggregate.AggregateType.Concat => ConcatAgg(f.name, rw, field, distinct = false)
          case lightdb.aggregate.AggregateType.ConcatDistinct => ConcatAgg(f.name, rw, field, distinct = true)
          case lightdb.aggregate.AggregateType.Group =>
            // Not supported yet (would require emitting multiple rows); keep deterministic placeholder.
            new AggComputer {
              override val name: String = f.name
              override val rw: RW[Any] = rw
              override def update(docId: String, doc: Doc): Unit = ()
              override def value(): Any = null
            }
        }
      }

      val processed: Task[Unit] =
        query.query.docs.stream
          .evalMap { doc =>
            Task {
              val id = doc._id.value
              computers.foreach(_.update(id, doc))
            }
          }
          .drain

      processed.map { _ =>
        val row: Map[String, Any] = computers.map(c => c.name -> c.value()).toMap
        query.filter match {
          case Some(f) if !evalAggFilter(row, f) =>
            rapid.Stream.empty
          case _ =>
            val json = obj(computers.map { c =>
              c.name -> c.rw.read(c.value())
            }: _*)
            rapid.Stream.emit(MaterializedAggregate[Doc, Model](json, store.model))
        }
      }
      }
    }

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] =
    // Match Query.count semantics: count ALL aggregate rows after HAVING, ignoring pagination.
    //
    // For grouped aggregates this counts groups; for global aggregates this is 0 or 1 depending on HAVING.
    aggregate(
      query.copy(
        query = query.query.clearLimit.clearPageSize.offset(0)
      )
    ).count
}


