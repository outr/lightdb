package lightdb.spatial

import lightdb.distance.Distance

case class DistanceAndDoc[Doc](doc: Doc, distance: Distance)