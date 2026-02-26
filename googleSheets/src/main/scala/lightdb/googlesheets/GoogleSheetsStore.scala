package lightdb.googlesheets

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.sheets.v4.{Sheets, SheetsScopes}
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.ServiceAccountCredentials
import lightdb.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
import rapid.Task

import java.io.FileInputStream
import java.nio.file.Path
import java.util.Collections

case class GoogleSheetsStore(
  spreadsheetId: String,
  service: Sheets
) extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = GoogleSheetsStoreImpl[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): GoogleSheetsStoreImpl[Doc, Model] = {
    val instance = new GoogleSheetsInstance(spreadsheetId, name, service)
    new GoogleSheetsStoreImpl[Doc, Model](name, path, model, storeMode, instance, db, this)
  }
}

object GoogleSheetsStore {
  def apply(spreadsheetId: String, credentialsPath: String): GoogleSheetsStore = {
    val service = createSheetsService(credentialsPath)
    GoogleSheetsStore(spreadsheetId, service)
  }

  private def createSheetsService(credentialsPath: String): Sheets = {
    val httpTransport = GoogleNetHttpTransport.newTrustedTransport()
    val jsonFactory = GsonFactory.getDefaultInstance
    val credentials = ServiceAccountCredentials
      .fromStream(new FileInputStream(credentialsPath))
      .createScoped(Collections.singletonList(SheetsScopes.SPREADSHEETS))
    new Sheets.Builder(httpTransport, jsonFactory, new HttpCredentialsAdapter(credentials))
      .setApplicationName("lightdb-google-sheets")
      .build()
  }
}

private[googlesheets] class GoogleSheetsStoreImpl[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  name: String,
  path: Option[Path],
  model: Model,
  val storeMode: StoreMode[Doc, Model],
  val instance: GoogleSheetsInstance,
  lightDB: LightDB,
  storeManager: StoreManager
) extends Store[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX = GoogleSheetsTransaction[Doc, Model]

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] =
    Task(GoogleSheetsTransaction(this, instance, parent, writeHandlerFactory))

  override protected def doDispose(): Task[Unit] = super.doDispose().next(instance.dispose())
}
