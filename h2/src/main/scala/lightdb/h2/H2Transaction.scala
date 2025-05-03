package lightdb.h2

import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.{SQLState, SQLStoreTransaction}
import lightdb.transaction.Transaction

case class H2Transaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: H2Store[Doc, Model],
                                                                            state: SQLState[Doc, Model],
                                                                            parent: Option[Transaction[Doc, Model]]) extends SQLStoreTransaction[Doc, Model]
