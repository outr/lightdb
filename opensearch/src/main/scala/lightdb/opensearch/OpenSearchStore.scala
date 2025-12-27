package lightdb.opensearch

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import fabric._
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.store.prefix.{PrefixScanningStore, PrefixScanningStoreManager}
import lightdb.store.{Collection, CollectionManager, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import rapid.Task

import java.nio.file.Path
import scala.language.implicitConversions

class OpenSearchStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                         path: Option[Path],
                                                                         model: Model,
                                                                         val storeMode: StoreMode[Doc, Model],
                                                                         lightDB: LightDB,
                                                                         storeManager: StoreManager,
                                                                         config: OpenSearchConfig,
                                                                         client: OpenSearchClient)
  extends Collection[Doc, Model](name, path, model, lightDB, storeManager)
    with PrefixScanningStore[Doc, Model] {
  override type TX = OpenSearchTransaction[Doc, Model]

  /**
   * OpenSearch can execute parent/child natively, but only if this collection is configured as part of a join-domain.
   * Default false until the join-domain feature is implemented.
   */
  override def supportsNativeExistsChild: Boolean =
    config.joinDomain.nonEmpty && config.joinRole.contains("parent")

  private lazy val indexAliasName: String = OpenSearchIndexName.default(lightDB.name, name, config)
  private lazy val writeAliasName: String = s"$indexAliasName${config.writeAliasSuffix}"

  // When enabled, the store will use a stable alias for all reads/writes and create a physical index on first init.
  // This provides a path to zero-downtime mapping changes via alias swap.
  private lazy val physicalIndexName: String = s"$indexAliasName${config.indexAliasSuffix}"

  lazy val readIndexName: String = indexAliasName
  lazy val deadLetterIndexName: String = OpenSearchDeadLetterIndexName.default(lightDB.name, name, config)

  lazy val writeIndexName: String = {
    if (config.useIndexAlias && config.useWriteAlias) writeAliasName
    else indexAliasName
  }

  // Backwards-compatible name used throughout the existing code: treat as the read target.
  lazy val indexName: String = readIndexName

  override protected def initialize(): Task[Unit] = super.initialize().next {
    // In a join domain, only the join-parent should create/own the physical index mapping.
    // Child collections share the same index and can safely skip index creation during initialization.
    val isJoinChild: Boolean = config.joinDomain.nonEmpty && config.joinRole.contains("child")

    def checkMappingFor(targetIndex: String): Task[Unit] = {
      if (isJoinChild || config.ignoreMappingHash) {
        Task.unit
      } else {
        val expected = OpenSearchTemplates.indexBody(model, fields, config, name, maxResultWindow = config.maxResultWindow)
          .asObj.get("mappings")
          .flatMap(_.asObj.get("_meta"))
          .flatMap(_.asObj.get("lightdb"))
          .flatMap(_.asObj.get("mapping_hash"))
          .map(_.asString)
        client.mappingHash(targetIndex).flatMap {
          case None =>
            // legacy index created before mapping hashes existed
            scribe.warn(s"OpenSearch index '$targetIndex' is missing mapping hash metadata; skipping mapping verification. " +
              s"Set lightdb.opensearch.ignoreMappingHash=false to enforce in the future (after migrating).")
            Task.unit
          case Some(actual) =>
            expected match {
              case Some(exp) if exp == actual =>
                Task.unit
              case Some(exp) =>
                Task.error(new RuntimeException(
                  s"OpenSearch mapping hash mismatch for '$targetIndex' (expected=$exp actual=$actual). " +
                    s"This usually means the DocumentModel/indexed fields changed incompatibly. " +
                    s"Create a new physical index and swap the read/write alias (recommended), or set " +
                    s"'lightdb.opensearch.ignoreMappingHash=true' to bypass this check."
                ))
              case None =>
                // Should not happen: our template always includes mapping_hash.
                Task.unit
            }
        }
      }
    }

    if (config.useIndexAlias) {
      // Ensure alias exists, creating a physical index + alias if needed.
      client.aliasExists(indexAliasName).flatMap {
        case true =>
          // Alias exists: ensure the optional write alias exists as well (useful if it was enabled later).
          // If alias points to a single index, verify mapping hash against that physical index.
          client.aliasTargets(indexAliasName).flatMap {
            case idx :: Nil => checkMappingFor(idx)
            case _ => Task.unit
          }.next(ensureWriteAliasIfConfigured())
        case false =>
          // For join-child stores, do not create the shared index or aliases. The join-parent should own index creation.
          if (isJoinChild) {
            Task.unit
          } else {
          // If an index already exists with the alias name, we cannot create an alias with the same name.
          // In that case, treat it as a non-aliased index (backward compatible).
          client.indexExists(indexAliasName).flatMap {
            case true =>
              // Backward compatible: a real index exists with the alias name. We cannot safely create aliases.
              checkMappingFor(indexAliasName)
            case false =>
              client.indexExists(physicalIndexName).flatMap {
                case true =>
                  checkMappingFor(physicalIndexName).next(createAliasesForPhysicalIndex())
                case false =>
                  val body = OpenSearchTemplates.indexBody(model, fields, config, name, maxResultWindow = config.maxResultWindow)
                  client.createIndex(physicalIndexName, body)
                    .next(checkMappingFor(physicalIndexName))
                    .next(createAliasesForPhysicalIndex())
              }
          }
          }
      }
    } else {
      client.indexExists(indexName).flatMap {
        case true => checkMappingFor(indexName)
        case false =>
          // For join-child stores, do not create the shared index. The join-parent should own index creation.
          if (isJoinChild) {
            Task.unit
          } else {
          // Minimal v1 index creation:
          // - set max_result_window high enough for LightDB offset-based streaming in tests/specs
          // - explicit mappings for indexed fields (strings/numbers/geo centers) to avoid sort/runtime 400s
          val body = OpenSearchTemplates.indexBody(model, fields, config, name, maxResultWindow = config.maxResultWindow)
          client.createIndex(indexName, body).next(checkMappingFor(indexName))
          }
      }
    }
  }

  private def createAliasesForPhysicalIndex(): Task[Unit] = {
    val actions = if (config.useWriteAlias) {
      arr(
        obj("add" -> obj("index" -> str(physicalIndexName), "alias" -> str(indexAliasName))),
        obj("add" -> obj("index" -> str(physicalIndexName), "alias" -> str(writeAliasName), "is_write_index" -> bool(true)))
      )
    } else {
      arr(
        obj("add" -> obj("index" -> str(physicalIndexName), "alias" -> str(indexAliasName)))
      )
    }
    client.updateAliases(obj("actions" -> actions))
  }

  private def ensureWriteAliasIfConfigured(): Task[Unit] = {
    if (!config.useWriteAlias) {
      Task.unit
    } else {
      client.aliasExists(writeAliasName).flatMap {
        case true =>
          Task.unit
        case false =>
          // If an index already exists with the write alias name, we cannot create an alias with the same name.
          // Treat as a no-op for backward compatibility.
          client.indexExists(writeAliasName).flatMap {
            case true => Task.unit
            case false =>
              client.aliasTargets(indexAliasName).flatMap {
                case idx :: Nil =>
                  client.updateAliases(obj("actions" -> arr(
                    obj("add" -> obj("index" -> str(idx), "alias" -> str(writeAliasName), "is_write_index" -> bool(true)))
                  )))
                case Nil =>
                  Task.unit
                case many =>
                  Task.error(new RuntimeException(
                    s"Write alias creation requires read alias '$indexAliasName' to point at exactly one index, " +
                      s"but it points at: ${many.mkString(", ")}. Create '$writeAliasName' manually or repoint '$indexAliasName'."
                  ))
              }
          }
      }
    }
  }

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] = Task {
    OpenSearchTransaction(this, config, client, parent)
  }
}

object OpenSearchStore extends CollectionManager with PrefixScanningStoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = OpenSearchStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] = {
    scribe.info(s"OpenSearchStore.create($name): begin")
    val config = OpenSearchConfig.from(db, name)
    scribe.info(s"OpenSearchStore.create($name): config loaded")
    val client = OpenSearchClient(config)
    scribe.info(s"OpenSearchStore.create($name): client created")
    new OpenSearchStore[Doc, Model](
      name = name,
      // OpenSearch is an external service; there is no local on-disk store directory for this collection.
      // Keep path=None to avoid implying filesystem persistence and to avoid creating unused directories in tests.
      path = None,
      model = model,
      storeMode = storeMode,
      lightDB = db,
      storeManager = this,
      config = config,
      client = client
    )
  }
}


