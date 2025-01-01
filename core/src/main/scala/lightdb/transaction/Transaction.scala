package lightdb.transaction

import lightdb.doc.Document
import lightdb.feature.FeatureSupport
import rapid._

final class Transaction[Doc <: Document[Doc]] extends FeatureSupport[TransactionKey] { transaction =>
  def commit(): Task[Unit] = features.map {
    case f: TransactionFeature => f.commit()
    case _ => Task.unit // Ignore
  }.tasks.unit

  def rollback(): Task[Unit] = features.map {
    case f: TransactionFeature => f.rollback()
    case _ => Task.unit // Ignore
  }.tasks.unit

  def close(): Task[Unit] = features.map {
    case f: TransactionFeature => f.close()
    case _ => Task.unit // Ignore
  }.tasks.unit
}