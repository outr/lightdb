package lightdb.lucene

import org.apache.lucene.search._
import org.apache.lucene.document._

case class LucenePaginatedIterator(searcher: IndexSearcher,
                                   query: Query,
                                   sort: Sort,
                                   pageSize: Int) extends Iterator[(Document, Double)] {
  private var currentPageIndex = 0
  private var currentDocs: Array[ScoreDoc] = Array.empty
  private var currentIndexInPage = 0
  private var exhausted = false

  private lazy val storedFields = searcher.storedFields()

  // Collect results page by page
  private def fetchPage(): Unit = {
    if (exhausted) return

    val start = currentPageIndex * pageSize

    val collectorManager = new TopFieldCollectorManager(sort, start + pageSize, null, Int.MaxValue, false)
    val topDocs = searcher.search(query, collectorManager)
    val totalHits = topDocs.totalHits.value

    if (start >= totalHits) {
      exhausted = true
      return
    }

    currentDocs = topDocs.scoreDocs.slice(start, start + pageSize)
    currentIndexInPage = 0
    currentPageIndex += 1
  }

  override def hasNext: Boolean = {
    if (currentIndexInPage < currentDocs.length) {
      true
    } else {
      fetchPage()
      currentIndexInPage < currentDocs.length
    }
  }

  override def next(): (Document, Double) = {
    if (!hasNext) {
      throw new NoSuchElementException("No more documents")
    }
    val scoreDoc = currentDocs(currentIndexInPage)
    val doc = storedFields.document(scoreDoc.doc)
    currentIndexInPage += 1
    doc -> scoreDoc.score.toDouble
  }
}