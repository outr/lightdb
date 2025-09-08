package lightdb.field

final class IndexingKey[T]

object IndexingKey {
  def apply[T]: IndexingKey[T] = new IndexingKey[T]
}