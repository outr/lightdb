package next

import fabric.Json

case class Query[Doc, Model <: DocModel[Doc]](collection: Collection[Doc, Model],
                                              filter: Option[Filter[Doc]] = None,
                                              sort: List[Sort] = Nil,
                                              offset: Int = 0,
                                              limit: Option[Int] = None,
                                              countTotal: Boolean = false) { query =>
  def filter(f: Model => Filter[Doc]): Query[Doc, Model] = {
    val filter = f(collection.model)
    val combined = this.filter match {
      case Some(current) => current && filter
      case None => filter
    }
    copy(filter = Some(combined))
  }

  def sort(sort: Sort*): Query[Doc, Model] = copy(sort = this.sort ::: sort.toList)

  object search {
    def apply[V](conversion: collection.store.Conversion[V])
                (implicit transaction: Transaction[Doc]): SearchResults[Doc, V] = collection.store.doSearch(
      query = query,
      conversion = conversion
    )

    def docs(implicit transaction: Transaction[Doc]): SearchResults[Doc, Doc] = apply(collection.store.Conversion.Doc)
    def value[F](field: Field[Doc, F])
                (implicit transaction: Transaction[Doc]): SearchResults[Doc, F] =
      apply(collection.store.Conversion.Value(field))
    def json(fields: Field[Doc, _]*)(implicit transaction: Transaction[Doc]): SearchResults[Doc, Json] =
      apply(collection.store.Conversion.Json(fields.toList))
    def converted[T](f: Doc => T)(implicit transaction: Transaction[Doc]): SearchResults[Doc, T] =
      apply(collection.store.Conversion.Converted(f))
  }
}