package lightdb

trait DocumentModel[D <: Document[D]] extends JsonMapping[D]
