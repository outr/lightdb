package spec

import lightdb.googlesheets.GoogleSheetsStore
import lightdb.store.StoreManager
import org.scalatest.{Args, Status, SucceededStatus}

@EmbeddedTest
class GoogleSheetsSpec extends AbstractKeyValueSpec {
  override val CreateRecords: Int = 50
  override def truncateAfter: Boolean = false

  private val spreadsheetId: String = sys.env.getOrElse("GOOGLE_SHEETS_SPREADSHEET_ID", "")
  private val credentialsPath: String = sys.env.getOrElse("GOOGLE_SHEETS_CREDENTIALS_PATH", "")

  override def storeManager: StoreManager = GoogleSheetsStore(spreadsheetId, credentialsPath)

  override def run(testName: Option[String], args: Args): Status = {
    if (spreadsheetId.isEmpty || credentialsPath.isEmpty) {
      scribe.warn("Skipping GoogleSheetsSpec: GOOGLE_SHEETS_SPREADSHEET_ID and GOOGLE_SHEETS_CREDENTIALS_PATH env vars must be set")
      SucceededStatus
    } else {
      super.run(testName, args)
    }
  }
}
