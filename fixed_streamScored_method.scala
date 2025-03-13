package lightdb

import fabric.Json
import lightdb.aggregate.{AggregateFunction, AggregateQuery}
import lightdb.distance.Distance
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.NonIndexedFieldException
import lightdb.facet.FacetQuery
import lightdb.field.Field._
import lightdb.field.{Field, IndexingState}
import lightdb.filter._
import lightdb.materialized.{MaterializedAndDoc, MaterializedIndex}
import lightdb.spatial.{DistanceAndDoc, Geo}
import lightdb.store.{Conversion, Store}
import lightdb.transaction.Transaction
import rapid.{Forge, Grouped, Task}

// This is a fixed implementation of the streamScored method
// The key change is in the calculation of the endOffset, which is now offset + limit
// This ensures that the method properly handles offset and limit by calculating the end position
// and using that as the upper bound for the offset in fetchAllPages

def streamScored(implicit transaction: Transaction[Doc]): rapid.Stream[(V, Double)] = {
  def fetchPage(offset: Int): Task[SearchResults[Doc, Model, V]] = {
    val pagedQuery = copy(offset = offset, countTotal = offset == this.offset)
    pagedQuery.search
  }

  def fetchAllPages(endOffset: Int, offset: Int): rapid.Stream[(V, Double)] = {
    if (offset >= endOffset) {
      rapid.Stream.empty
    } else {
      rapid.Stream.force(fetchPage(offset).flatMap { searchResults =>
        val nextPageStream = fetchAllPages(endOffset, offset + pageSize)
        Task.pure(searchResults.streamWithScore ++ nextPageStream)
      })
    }
  }

  rapid.Stream.force(fetchPage(offset).flatMap { firstPageResults =>
    val total = firstPageResults.total.get
    scribe.info(s"TOTAL: $total")
    val endOffset = this.limit match {
      case Some(l) => offset + math.min(l, total - offset)
      case None => total
    }
    Task.pure(fetchAllPages(endOffset, offset))
  })
}