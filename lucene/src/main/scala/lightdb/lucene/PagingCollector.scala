package lightdb.lucene

import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.{CollectorManager, Scorable, ScoreDoc, ScoreMode, Scorer, SimpleCollector, TopDocs}

import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala

case class PagingCollector(offset: Int, limit: Int) extends SimpleCollector {
  private var totalHits = 0
  private var docBase = 0
  private var scorer: Scorable = _
  private var _documents = List.empty[ScoreDoc]

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    this.docBase = context.docBase
  }

  override def setScorer(scorer: Scorable): Unit = this.scorer = scorer

  override def collect(doc: Int): Unit = {
    totalHits += 1
    if (totalHits > offset && totalHits - offset < limit) {
      val score = scorer.score()
      val document = new ScoreDoc(doc + docBase, score)
      _documents = document :: _documents
    }
  }

  override def scoreMode(): ScoreMode = ScoreMode.TOP_SCORES

  def documents: List[ScoreDoc] = _documents.reverse

  def total: Int = totalHits
}

case class PagingCollectorManager(offset: Int, limit: Int) extends CollectorManager[PagingCollector, PagedResults] {
  override def newCollector(): PagingCollector = PagingCollector(offset, limit)

  override def reduce(collectors: util.Collection[PagingCollector]): PagedResults = collectors.asScala.toList match {
    case c :: Nil => PagedResults(offset, limit, c.documents, c.total)
    case list => throw new RuntimeException(s"Unable to reduce multiple collectors: $list")
  }
}

case class PagedResults(offset: Int, limit: Int, documents: List[ScoreDoc], total: Int)