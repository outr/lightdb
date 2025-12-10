package benchmark.jmh.imdb

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.field.Field._
import lightdb.id.Id

case class TitleAka(titleId: String,
                    ordering: Int,
                    title: String,
                    region: Option[String],
                    language: Option[String],
                    types: List[String],
                    attributes: List[String],
                    isOriginalTitle: Option[Boolean],
                    _id: Id[TitleAka] = Id[TitleAka]()) extends Document[TitleAka]

object TitleAka extends DocumentModel[TitleAka] with JsonConversion[TitleAka] {
  implicit val rw: RW[TitleAka] = RW.gen

  val titleId: I[String] = field.index("titleId", _.titleId)
  val ordering: I[Int] = field.index("ordering", _.ordering)
  val title: I[String] = field.index("title", _.title)
  val region: I[Option[String]] = field.index("region", _.region)
  val language: I[Option[String]] = field.index("language", _.language)
  val types: F[List[String]] = field("types", _.types)
  val attributes: F[List[String]] = field("attributes", _.attributes)
  val isOriginalTitle: I[Option[Boolean]] = field.index("isOriginalTitle", _.isOriginalTitle)
}

case class TitleBasics(tconst: String,
                       titleType: String,
                       primaryTitle: String,
                       originalTitle: String,
                       isAdult: Boolean,
                       startYear: Int,
                       endYear: Int,
                       runtimeMinutes: Int,
                       genres: List[String],
                       _id: Id[TitleBasics] = Id[TitleBasics]()) extends Document[TitleBasics]

object TitleBasics extends DocumentModel[TitleBasics] with JsonConversion[TitleBasics] {
  implicit val rw: RW[TitleBasics] = RW.gen

  val tconst: F[String] = field("tconst", _.tconst)
  val titleType: F[String] = field("titleType", _.titleType)
  val primaryTitle: F[String] = field("primaryTitle", _.primaryTitle)
  val originalTitle: F[String] = field("originalTitle", _.originalTitle)
  val isAdult: F[Boolean] = field("isAdult", _.isAdult)
  val startYear: F[Int] = field("startYear", _.startYear)
  val endYear: F[Int] = field("endYear", _.endYear)
  val runtimeMinutes: F[Int] = field("runtimeMinutes", _.runtimeMinutes)
  val genres: F[List[String]] = field("genres", _.genres)
}



