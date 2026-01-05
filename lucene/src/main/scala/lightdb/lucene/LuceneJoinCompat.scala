package lightdb.lucene

import org.apache.lucene.search.Query

/**
 * Compat shim around lucene-join.
 *
 * Some tooling environments (e.g. IDE presentation compilers) can get confused about the
 * `org.apache.lucene.search.join.*` package even when sbt compiles and tests pass. Using
 * reflection keeps the runtime behavior (when lucene-join is on the classpath) while
 * avoiding hard compile-time references.
 */
private[lucene] object LuceneJoinCompat {
  private val scoreModeClass = Class.forName("org.apache.lucene.search.join.ScoreMode")
  private val bitSetProducerClass = Class.forName("org.apache.lucene.search.join.BitSetProducer")
  private val queryBitSetProducerClass = Class.forName("org.apache.lucene.search.join.QueryBitSetProducer")
  private val toParentBlockJoinQueryClass = Class.forName("org.apache.lucene.search.join.ToParentBlockJoinQuery")

  private val queryBitSetProducerCtor = queryBitSetProducerClass.getConstructor(classOf[Query])
  private val toParentBlockJoinQueryCtor =
    toParentBlockJoinQueryClass.getConstructor(classOf[Query], bitSetProducerClass, scoreModeClass)

  private val scoreModeNone: AnyRef =
    scoreModeClass.getMethod("valueOf", classOf[String]).invoke(null, "None").asInstanceOf[AnyRef]

  def queryBitSetProducer(parentTypeQuery: Query): AnyRef =
    queryBitSetProducerCtor.newInstance(parentTypeQuery).asInstanceOf[AnyRef]

  def toParentBlockJoinQuery(childQuery: Query, parentProducer: AnyRef): Query =
    toParentBlockJoinQueryCtor
      .newInstance(childQuery, parentProducer, scoreModeNone)
      .asInstanceOf[Query]
}


