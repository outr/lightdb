package lightdb.model

import lightdb.{LightDB, RecordDocument}

abstract class RecordDocumentCollection[D <: RecordDocument[D]](collectionName: String, db: LightDB) extends Collection[D](collectionName, db) with RecordDocumentModel[D]