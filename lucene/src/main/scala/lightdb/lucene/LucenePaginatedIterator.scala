package lightdb.lucene

import org.apache.lucene.document._
import org.apache.lucene.index.StoredFields
import org.apache.lucene.search._

case class LucenePaginatedIterator(searcher: IndexSearcher,
                                   query: Query,
                                   sort: Sort,
                                   pageSize: Int,
                                   scoreDocs: Boolean) extends Iterator[(Document, Double)] {
  private var currentPageIndex = 0
  private var currentDocs: Array[ScoreDoc] = Array.empty
  private var currentIndexInPage = 0
  private var exhausted = false
  private var totalHits = -1

  lazy val storedFields: StoredFields = searcher.storedFields()

  def total: Int = {
    if (totalHits == -1) {
      fetchPage()
    }
    totalHits
  }

  // Collect results page by page
  private def fetchPage(): Unit = {
    if (exhausted) return

    val start = currentPageIndex * pageSize

    val threshold = if (totalHits == -1) Int.MaxValue else 0

    val after = currentDocs.lastOption.map(_.asInstanceOf[FieldDoc]).orNull
    val collectorManager = new TopFieldCollectorManager(sort, pageSize, after, threshold)
    val topDocs = searcher.search(query, collectorManager)

    if (currentPageIndex == 0) totalHits = topDocs.totalHits.value.toInt

    if (start >= totalHits) {
      exhausted = true
      return
    }

    currentDocs = topDocs.scoreDocs
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