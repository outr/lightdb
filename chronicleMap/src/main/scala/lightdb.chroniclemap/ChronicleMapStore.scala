package lightdb.chroniclemap

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
import net.openhft.chronicle.map.ChronicleMap
import net.openhft.compiler.CompilerUtils
import rapid.Task
import scribe.{Level, Logger}

import java.net.URLClassLoader
import java.nio.file.{Files, Path, Paths}

class ChronicleMapStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                           path: Option[Path],
                                                                           model: Model,
                                                                           val storeMode: StoreMode[Doc, Model],
                                                                           lightDB: LightDB,
                                                                           storeManager: StoreManager) extends Store[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX = ChronicleMapTransaction[Doc, Model]

  private lazy val db: ChronicleMap[String, String] = {
    ChronicleMapStore.compilerClasspathSynced
    val b = ChronicleMap
      .of(classOf[String], classOf[String])
      .name(name)
      .entries(1_000_000)
      .averageKeySize(32)
      .averageValueSize(5 * 1024)
      .maxBloatFactor(2.0)
      .sparseFile(true)
    path match {
      case Some(d) =>
        Files.createDirectories(d.getParent)
        b.createPersistedTo(d.toFile)
      case None => b.create()
    }
  }

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] =
    Task(ChronicleMapTransaction(this, db, parent, writeHandlerFactory))

  override protected def initialize(): Task[Unit] = super.initialize().next(Task(db))

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task {
    db.close()
  })
}

object ChronicleMapStore extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = ChronicleMapStore[Doc, Model]

  /**
   * ChronicleMap generates its marshaller classes at runtime and compiles them with javac, using the
   * `java.class.path` system property as the compiler classpath. Launchers that load the application
   * through a URLClassLoader instead of `-cp` (sbt 2 forked tests, for one) leave that property with
   * only the launcher's own jars, and compilation fails with `CompletionFailure`s for ordinary
   * dependencies. Hand the class loader chain's entries to the compiler through OpenHFT's own hook
   * (it de-duplicates against the property) before the first map is created.
   */
  private lazy val compilerClasspathSynced: Unit = {
    val loaders = Iterator.iterate(Thread.currentThread.getContextClassLoader)(_.getParent).takeWhile(_ != null) ++
      Iterator.iterate(getClass.getClassLoader)(_.getParent).takeWhile(_ != null)
    loaders.collect { case ucl: URLClassLoader => ucl }.flatMap(_.getURLs).filter(_.getProtocol == "file").foreach { url =>
      CompilerUtils.addClassPath(Paths.get(url.toURI).toString)
    }
  }

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] = {
    Logger("net.openhft.chronicle").withMinimumLevel(Level.Warn).replace()

    new ChronicleMapStore[Doc, Model](name, path, model, storeMode, db, this)
  }
}
