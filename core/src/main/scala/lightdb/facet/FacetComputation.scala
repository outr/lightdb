package lightdb.facet

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.FacetField
import lightdb.field.IndexingState

import scala.collection.mutable

object FacetComputation {
  def fromDocs[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    docs: Iterable[Doc],
    facetQueries: List[FacetQuery[Doc]],
    model: Model
  ): Map[FacetField[Doc], FacetResult] = {
    val facetFields: List[FacetField[Doc]] = facetQueries.map(_.field).distinct
    if facetFields.isEmpty then {
      Map.empty
    } else {
      val countsByField = mutable.HashMap.empty[FacetField[Doc], mutable.HashMap[String, Int]]
      facetFields.foreach { ff =>
        countsByField.put(ff, mutable.HashMap.empty[String, Int])
      }

      def bump(ff: FacetField[Doc], key: String): Unit = {
        val map = countsByField(ff)
        map.updateWith(key) {
          case Some(v) => Some(v + 1)
          case None => Some(1)
        }
      }

      val queryByField: Map[FacetField[Doc], FacetQuery[Doc]] =
        facetQueries.foldLeft(Map.empty[FacetField[Doc], FacetQuery[Doc]]) {
          case (m, fq) => m.updated(fq.field, fq)
        }

      val state = new IndexingState
      docs.foreach { doc =>
        facetFields.foreach { ff =>
          val fq = queryByField(ff)
          val values: List[FacetValue] = ff.get(doc, ff, state)
          if values.isEmpty then {
            if ff.hierarchical && fq.path.isEmpty then bump(ff, "$ROOT$")
          } else {
            values.foreach { fv =>
              val path = fv.path
              if ff.hierarchical then {
                if path.startsWith(fq.path) then {
                  val child = if path.length == fq.path.length then "$ROOT$" else path(fq.path.length)
                  bump(ff, child)
                }
              } else {
                val child = path.headOption.getOrElse("$ROOT$")
                bump(ff, child)
              }
            }
          }
        }
      }

      queryByField.map { case (ff, fq) =>
        val counts = countsByField(ff).toMap
        val childrenLimit = fq.childrenLimit.getOrElse(Int.MaxValue)
        val sorted = counts.iterator
          .filter(_._1 != "$ROOT$")
          .toList
          .sortBy { case (value, count) => (-count, value) }
        val top = sorted.take(childrenLimit).map { case (value, count) => FacetResultValue(value, count) }
        val totalCount = top.map(_.count).sum
        val childCount = counts.keySet.size
        ff -> FacetResult(top, childCount = childCount, totalCount = totalCount)
      }
    }
  }
}
