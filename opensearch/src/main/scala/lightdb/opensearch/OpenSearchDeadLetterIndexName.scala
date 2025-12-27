package lightdb.opensearch

import lightdb.opensearch.client.OpenSearchConfig

object OpenSearchDeadLetterIndexName {
  /**
   * Dead-letter indices are per-collection (not per-joinDomain) so errors can be traced back to the owning store.
   */
  def default(dbName: String, collectionName: String, config: OpenSearchConfig): String = {
    val prefix = config.indexPrefix.map(p => s"${normalize(p)}_").getOrElse("")
    s"$prefix${normalize(dbName)}_${normalize(collectionName)}${config.deadLetterIndexSuffix}"
  }

  private def normalize(s: String): String = s
    .trim
    .toLowerCase
    .replace(' ', '_')
}


