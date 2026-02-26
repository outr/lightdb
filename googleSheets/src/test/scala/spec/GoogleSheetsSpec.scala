package spec

import lightdb.googlesheets.GoogleSheetsStore
import lightdb.store.StoreManager

@EmbeddedTest
class GoogleSheetsSpec extends AbstractKeyValueSpec {
  override val CreateRecords: Int = 50
  override def truncateAfter: Boolean = false

  private val spreadsheetId: String = sys.env.getOrElse("GOOGLE_SHEETS_SPREADSHEET_ID", "")
  private val credentialsPath: String = sys.env.getOrElse("GOOGLE_SHEETS_CREDENTIALS_PATH", "")

  assume(spreadsheetId.nonEmpty && credentialsPath.nonEmpty,
    "GOOGLE_SHEETS_SPREADSHEET_ID and GOOGLE_SHEETS_CREDENTIALS_PATH env vars must be set")

  override def storeManager: StoreManager = GoogleSheetsStore(spreadsheetId, credentialsPath)
}
