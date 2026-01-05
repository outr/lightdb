package lightdb.opensearch

import java.util.concurrent.ConcurrentHashMap

/**
 * Programmatic join-domain configuration registry.
 *
 * This allows applications to configure join-domains in code (without JVM system properties / Profig), while keeping OpenSearchConfig
 * as the single point of truth for store behavior.
 *
 * Notes:
 * - This is process-local (in-memory).
 * - Intended for server apps and tests where configuration is set up once during bootstrap.
 */
object OpenSearchJoinDomainRegistry {
  case class Entry(joinDomain: String,
                   joinRole: String,                         // "parent" | "child"
                   joinChildren: List[String] = Nil,          // parent only
                   joinParentField: Option[String] = None,    // child only
                   joinFieldName: String = "__lightdb_join")

  private val byDbAndStore = new ConcurrentHashMap[String, Entry]()

  private def key(dbName: String, storeName: String): String =
    s"${dbName.trim}::${storeName.trim}"

  def register(dbName: String,
               joinDomain: String,
               parentStoreName: String,
               childJoinParentFields: Map[String, String],
               joinFieldName: String = "__lightdb_join"): Unit = {
    val children = childJoinParentFields.keys.toList.sorted
    put(dbName, parentStoreName, Entry(
      joinDomain = joinDomain,
      joinRole = "parent",
      joinChildren = children,
      joinParentField = None,
      joinFieldName = joinFieldName
    ))
    childJoinParentFields.foreach { case (childStoreName, parentField) =>
      put(dbName, childStoreName, Entry(
        joinDomain = joinDomain,
        joinRole = "child",
        joinChildren = Nil,
        joinParentField = Some(parentField),
        joinFieldName = joinFieldName
      ))
    }
  }

  def get(dbName: String, storeName: String): Option[Entry] =
    Option(byDbAndStore.get(key(dbName, storeName)))

  def clear(): Unit =
    byDbAndStore.clear()

  def withRegistration[A](dbName: String,
                          joinDomain: String,
                          parentStoreName: String,
                          childJoinParentFields: Map[String, String],
                          joinFieldName: String = "__lightdb_join")(f: => A): A = {
    val keys = (parentStoreName :: childJoinParentFields.keys.toList).distinct
    val prev = keys.map { s => key(dbName, s) -> Option(byDbAndStore.get(key(dbName, s))) }.toMap
    register(dbName, joinDomain, parentStoreName, childJoinParentFields, joinFieldName)
    try {
      f
    } finally {
      prev.foreach {
        case (k, Some(v)) => byDbAndStore.put(k, v)
        case (k, None) => byDbAndStore.remove(k)
      }
    }
  }

  private def put(dbName: String, storeName: String, entry: Entry): Unit =
    byDbAndStore.put(key(dbName, storeName), entry)
}


