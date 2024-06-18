package lightdb.query

import lightdb.Document
import squants.space.Length

case class DistanceAndDoc[D <: Document[D]](doc: D, distance: Length)