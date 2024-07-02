package lightdb.spatial

import lightdb.document.Document
import squants.space.Length

case class DistanceAndDoc[D <: Document[D]](doc: D, distance: Length)