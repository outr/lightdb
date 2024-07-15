package lightdb.sql

import lightdb.doc.{Document, DocumentModel}

import java.sql.ResultSet

/**
 * Mix-in to DocModel to provide overriding optimizing conversion support.
 */
trait SQLConversion[Doc <: Document[Doc]] extends DocumentModel[Doc] {
  def convertFromSQL(rs: ResultSet): Doc

  override def map2Doc(map: Map[String, Any]): Doc =
    throw new RuntimeException("Should not be used in favor of convertFromSQL")
}
