package lightdb.api

import fabric.*
import lightdb.LightDB
import lightdb.doc.DocumentModel
import lightdb.error.DuplicateIdException
import lightdb.store.{Collection, Store, StoreMode}
import rapid.*

/**
 * Diagnostic / admin HTTP surface for a [[LightDB]] instance.
 *
 * The handler is intentionally framework-agnostic: callers translate an
 * inbound HTTP request into an [[ApiRequest]] and serialize the returned
 * [[ApiResponse]] back to the wire. The `api-spice` module supplies that
 * adapter for Spice.
 *
 * Authorization is deliberately not in scope. Wrap this handler with whatever
 * middleware the embedding application uses; if a wrapper wants structured
 * access to the inbound operation (to allow/deny by store, by mutation, etc.)
 * it can call [[parse]] and inspect the resulting [[Operation]] without
 * re-parsing the URL.
 *
 * Endpoint reference (v1):
 *
 *   GET    /stores                            → list registered stores
 *   GET    /stores/{name}/count               → total document count
 *   GET    /stores/{name}                     → paged document listing
 *   GET    /stores/{name}/stream              → full-scan SSE stream
 *   GET    /stores/{name}/{id}                → fetch one document
 *   POST   /stores/{name}      (body: doc)    → upsert (create-or-replace)
 *   POST   /stores/{name}/truncate            → drop every document
 *   DELETE /stores/{name}/{id}                → delete one document
 */
trait LightDBHttpHandler {
  def db: LightDB

  /** Maximum documents returned by a single paged-listing call. The query
   * parameter `limit` is clamped to this value. */
  def maxPageSize: Int = 1000

  /** Default page size when the request omits `limit`. */
  def defaultPageSize: Int = 100

  /** Parse a request into a typed [[Operation]] without executing it.
   * Wrappers use this for policy decisions. */
  def parse(request: ApiRequest): Operation = LightDBHttpHandler.parse(
    request,
    defaultPageSize = defaultPageSize,
    maxPageSize = maxPageSize
  )

  /** Execute a parsed operation against the database. */
  def execute(op: Operation): Task[ApiResponse] = op match {
    case Operation.ListStores               => listStores
    case Operation.Count(name)              => withStore(name)(count)
    case Operation.Get(name, id)            => withStore(name)(getOne(_, id))
    case Operation.ListDocs(name, lim, off) => withStore(name)(listDocs(_, lim, off))
    case Operation.Stream(name)             => withStore(name)(streamDocs)
    case Operation.Upsert(name, body)       => withStore(name)(upsert(_, body))
    case Operation.Delete(name, id)         => withStore(name)(deleteOne(_, id))
    case Operation.Truncate(name)           => withStore(name)(truncate)
    case Operation.Unknown(method, path)    =>
      Task.pure(ApiResponse.notFound(s"No handler for $method $path"))
  }

  /** Default entry point: parse then execute. */
  def handle(request: ApiRequest): Task[ApiResponse] = execute(parse(request))

  // -- Dispatch helpers --------------------------------------------------------------------------------

  private type AnyStore = Store[_, _ <: DocumentModel[_]]

  private def withStore(name: String)(f: AnyStore => Task[ApiResponse]): Task[ApiResponse] =
    db.storesByNames(name).headOption match {
      case Some(store) => f(store).handleError { t =>
        scribe.warn(s"LightDBHttpHandler: dispatch failed on store=$name", t)
        Task.pure(ApiResponse.serverError(s"${t.getClass.getSimpleName}: ${t.getMessage}"))
      }
      case None => Task.pure(ApiResponse.notFound(s"Unknown store: $name"))
    }

  private def listStores: Task[ApiResponse] = Task {
    val entries = db.stores.map { s =>
      obj(
        "name" -> str(s.name),
        "mode" -> str(s.storeMode match {
          case _: StoreMode.All[_, _]     => "all"
          case _: StoreMode.Indexes[_, _] => "indexes"
        }),
        "collection" -> bool(s.isInstanceOf[Collection[_, _]])
      )
    }
    ApiResponse.ok(obj("stores" -> arr(entries*)))
  }

  private def count(store: AnyStore): Task[ApiResponse] =
    store.transaction(_.count).map(c => ApiResponse.ok(obj("count" -> num(c))))

  private def getOne(store: AnyStore, id: String): Task[ApiResponse] =
    store.transaction(_.jsonGet(id)).map {
      case Some(json) => ApiResponse.ok(json)
      case None       => ApiResponse.notFound(s"No document with id=$id in store=${store.name}")
    }

  private def listDocs(store: AnyStore, limit: Int, offset: Int): Task[ApiResponse] =
    store.transaction(_.jsonStream.drop(offset).take(limit).toList).map { docs =>
      ApiResponse.ok(obj(
        "store"  -> str(store.name),
        "offset" -> num(offset),
        "limit"  -> num(limit),
        "count"  -> num(docs.length),
        "docs"   -> arr(docs*)
      ))
    }

  private def streamDocs(store: AnyStore): Task[ApiResponse] = Task {
    val builder = store.transaction
    // Tie the transaction lifetime to the SSE stream — the binding consumes the stream lazily,
    // so we can't release the tx eagerly the way `transaction.apply` would.
    val events: rapid.Stream[Json] = rapid.Stream.force(
      builder.create().map(tx => tx.jsonStream.guarantee(builder.release(tx)))
    )
    ApiResponse(200, ApiContent.Stream(events))
  }

  private def upsert(store: AnyStore, body: Json): Task[ApiResponse] = {
    // Validate the body against the model's RW before opening a transaction.
    // RW.write throws on malformed JSON (missing required fields, type mismatches);
    // catch here so we can return 400 instead of a generic 500 from inside upsertJson.
    val validation: Either[Throwable, Unit] =
      try { val _ = store.model.rw.write(body); Right(()) }
      catch { case t: Throwable => Left(t) }
    validation match {
      case Left(t) => Task.pure(ApiResponse.badRequest(s"Invalid document JSON: ${t.getMessage}"))
      case Right(_) =>
        idOf(body) match {
          case None => Task.pure(ApiResponse.badRequest("Document body is missing `_id`"))
          case Some(id) =>
            store.transaction(_.upsertJson(rapid.Stream.emit(body)))
              .flatMap(_ => store.transaction(_.jsonGet(id)))
              .map {
                case Some(json) => ApiResponse.ok(json)
                case None       => ApiResponse.serverError("Upserted document could not be read back")
              }
              .handleError {
                case dup: DuplicateIdException => Task.pure(ApiResponse.conflict(dup.getMessage))
                case t                         => Task.pure(ApiResponse.serverError(s"${t.getClass.getSimpleName}: ${t.getMessage}"))
              }
        }
    }
  }

  private def deleteOne(store: AnyStore, id: String): Task[ApiResponse] =
    store.transaction { tx =>
      tx.jsonGet(id).flatMap {
        case None => Task.pure(ApiResponse.notFound(s"No document with id=$id in store=${store.name}"))
        case Some(_) => tx.jsonDelete(id).map { _ =>
          ApiResponse.ok(obj("deleted" -> bool(true), "id" -> str(id)))
        }
      }
    }

  private def truncate(store: AnyStore): Task[ApiResponse] =
    store.transaction(_.truncate).map(n => ApiResponse.ok(obj("truncated" -> num(n))))

  private def idOf(body: Json): Option[String] = body match {
    case o: Obj => o.value.get("_id").collect { case Str(v, _) => v }
    case _      => None
  }
}

object LightDBHttpHandler {

  /** Parses an [[ApiRequest]] into an [[Operation]] using the v1 routing
   * table. Returns [[Operation.Unknown]] when nothing matches — callers
   * decide whether that's a 404, a chain-through, or something else. */
  def parse(request: ApiRequest, defaultPageSize: Int = 100, maxPageSize: Int = 1000): Operation = {
    val segments = request.path
      .split('/')
      .iterator
      .filter(_.nonEmpty)
      .toList
    val method = request.method.toUpperCase

    segments match {
      case "stores" :: Nil if method == "GET" => Operation.ListStores

      case "stores" :: name :: Nil if method == "GET" =>
        val limit = request.params.get("limit").flatMap(_.toIntOption)
          .getOrElse(defaultPageSize)
          .max(0)
          .min(maxPageSize)
        val offset = request.params.get("offset").flatMap(_.toIntOption).getOrElse(0).max(0)
        Operation.ListDocs(name, limit, offset)

      case "stores" :: name :: Nil if method == "POST" =>
        request.body match {
          case Some(json) => Operation.Upsert(name, json)
          case None       => Operation.Unknown(method, request.path)
        }

      case "stores" :: name :: "count" :: Nil if method == "GET" =>
        Operation.Count(name)

      case "stores" :: name :: "stream" :: Nil if method == "GET" =>
        Operation.Stream(name)

      case "stores" :: name :: "truncate" :: Nil if method == "POST" =>
        Operation.Truncate(name)

      case "stores" :: name :: id :: Nil if method == "GET" =>
        Operation.Get(name, id)

      case "stores" :: name :: id :: Nil if method == "DELETE" =>
        Operation.Delete(name, id)

      case _ => Operation.Unknown(method, request.path)
    }
  }
}
