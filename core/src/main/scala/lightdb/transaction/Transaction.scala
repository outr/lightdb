package lightdb.transaction

import lightdb.doc.Document
import lightdb.feature.FeatureSupport

final class Transaction[Doc <: Document[Doc]] extends FeatureSupport[TransactionKey] { transaction =>
  def commit(): Unit = {
    features.foreach {
      case f: TransactionFeature => f.commit()
      case _ => // Ignore
    }
  }

  def rollback(): Unit = {
    features.foreach {
      case f: TransactionFeature => f.rollback()
      case _ => // Ignore
    }
  }

  def close(): Unit = {
    features.foreach {
      case f: TransactionFeature => f.close()
      case _ => // Ignore
    }
  }
}