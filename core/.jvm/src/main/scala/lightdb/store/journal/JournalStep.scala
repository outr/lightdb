package lightdb.store.journal

/** The steps of a journalled commit, reported to [[Journal.faultHook]] (for fault-injection tests). */
sealed trait JournalStep

object JournalStep {
  case class BeforeIntent(sequence: Long) extends JournalStep
  case class AfterIntent(sequence: Long) extends JournalStep
  case class BeforeApply(sequence: Long, attempt: Int) extends JournalStep
  case class AfterApplyStore(sequence: Long, store: String) extends JournalStep
  case class BeforeDelete(sequence: Long) extends JournalStep
  case class AfterDelete(sequence: Long) extends JournalStep
}
