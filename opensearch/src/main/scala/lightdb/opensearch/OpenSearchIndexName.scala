package lightdb.opensearch

import lightdb.opensearch.client.OpenSearchConfig

object OpenSearchIndexName {
  def default(dbName: String, collectionName: String, config: OpenSearchConfig): String = {
    val prefix = config.indexPrefix.map(p => s"${normalize(p)}_").getOrElse("")
    val indexPart = config.joinDomain.getOrElse(collectionName)
    s"$prefix${normalize(dbName)}_${normalize(indexPart)}"
  }

  private def normalize(s: String): String = s
    .trim
    .toLowerCase
    .replace(' ', '_')
}


