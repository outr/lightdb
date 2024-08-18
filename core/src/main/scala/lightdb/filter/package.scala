package lightdb

package object filter {
  implicit class ListFilterExtras[V, Doc, Filter](fs: FilterSupport[List[V], Doc, Filter]) {
    def ===(value: V): Filter = fs.is(List(value))
    def is(value: V): Filter = fs.is(List(value))
  }
  implicit class SetFilterExtras[V, Doc, Filter](fs: FilterSupport[Set[V], Doc, Filter]) {
    def ===(value: V): Filter = fs.is(Set(value))
    def is(value: V): Filter = fs.is(Set(value))
  }
}
