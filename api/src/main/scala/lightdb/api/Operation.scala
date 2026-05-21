package lightdb.api

import fabric.Json

/**
 * Parsed representation of an inbound API call.
 *
 * Wrappers that want to enforce access policy can call
 * [[LightDBHttpHandler.parse]] to get an `Operation`, inspect it (store name,
 * mutation vs read, etc.), and either reject the request up front or forward
 * to `execute`. This avoids re-parsing the URL in multiple layers.
 */
sealed trait Operation {

  /** The store this operation targets, if any. `ListStores` and `Unknown`
   * return `None`. */
  def storeName: Option[String]

  /** True if this operation mutates state (insert / upsert / delete / truncate). */
  def isMutation: Boolean
}

object Operation {
  /** `GET /stores` — enumerate registered stores. */
  case object ListStores extends Operation {
    val storeName: Option[String] = None
    val isMutation: Boolean = false
  }

  /** `GET /stores/{store}/count` — total document count. */
  case class Count(store: String) extends Operation {
    val storeName: Option[String] = Some(store)
    val isMutation: Boolean = false
  }

  /** `GET /stores/{store}/{id}` — fetch a single document by id. */
  case class Get(store: String, id: String) extends Operation {
    val storeName: Option[String] = Some(store)
    val isMutation: Boolean = false
  }

  /** `GET /stores/{store}?limit=&offset=` — paged listing of documents. */
  case class ListDocs(store: String, limit: Int, offset: Int) extends Operation {
    val storeName: Option[String] = Some(store)
    val isMutation: Boolean = false
  }

  /** `GET /stores/{store}/stream` — full-scan stream over the store (SSE). */
  case class Stream(store: String) extends Operation {
    val storeName: Option[String] = Some(store)
    val isMutation: Boolean = false
  }

  /** `POST /stores/{store}` with body = JSON document — create-or-replace. */
  case class Upsert(store: String, body: Json) extends Operation {
    val storeName: Option[String] = Some(store)
    val isMutation: Boolean = true
  }

  /** `DELETE /stores/{store}/{id}` — remove a single document. */
  case class Delete(store: String, id: String) extends Operation {
    val storeName: Option[String] = Some(store)
    val isMutation: Boolean = true
  }

  /** `POST /stores/{store}/truncate` — remove every document in the store. */
  case class Truncate(store: String) extends Operation {
    val storeName: Option[String] = Some(store)
    val isMutation: Boolean = true
  }

  /** Anything the default parser doesn't recognize. */
  case class Unknown(method: String, path: String) extends Operation {
    val storeName: Option[String] = None
    val isMutation: Boolean = false
  }
}
