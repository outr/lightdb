package lightdb.opensearch

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import fabric.*
import lightdb.doc.ParentChildSupport
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.store.prefix.{PrefixScanningStore, PrefixScanningStoreManager}
import lightdb.store.{Collection, CollectionManager, StoreManager, StoreMode}
import lightdb.store.write.WriteOp
import lightdb.transaction.{Transaction, WriteHandler}
import lightdb.transaction.batch.BatchConfig
import lightdb.transaction.handler.BufferedWriteHandler
import rapid.{Task, logger}

import java.nio.file.Path
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.language.implicitConversions

class OpenSearchStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                         path: Option[Path],
                                                                         model: Model,
                                                                         val storeMode: StoreMode[Doc, Model],
                                                                         lightDB: LightDB,
                                                                         storeManager: StoreManager)
  extends Collection[Doc, Model](name, path, model, lightDB, storeManager)
    with PrefixScanningStore[Doc, Model] {
  override type TX = OpenSearchTransaction[Doc, Model]

  override def defaultBatchConfig: BatchConfig = BatchConfig.StoreNative

  override protected def flushOps(transaction: Transaction[Doc, Model], ops: Seq[WriteOp[Doc]]): Task[Unit] =
    transaction.asInstanceOf[TX].flushOps(ops)

  override protected def createNativeWriteHandler(transaction: Transaction[Doc, Model]): WriteHandler[Doc, Model] =
    new BufferedWriteHandler(config.bulkMaxDocs, ops => flushOps(transaction, ops))

  private def ensureAutoJoinDomainRegistry(): Unit = {
    if OpenSearchJoinDomainRegistry.get(lightDB.name, name).nonEmpty then {
      ()
    } else {
      def hasExplicitJoinKeys(storeName: String): Boolean = {
        val prefix = s"lightdb.opensearch.$storeName."
        val keys = List(
          s"${prefix}joinDomain",
          s"${prefix}joinRole",
          s"${prefix}joinChildren",
          s"${prefix}joinChildParentFields",
          s"${prefix}joinParentField"
        )
        keys.exists(k => profig.Profig(k).exists())
      }

      if hasExplicitJoinKeys(name) then ()
      else {
      case class Candidate(parentStoreName: String, joinDomain: String, joinFieldName: String, childStoreName: String, childParentFieldName: String)

      def profigString(key: String): Option[String] =
        profig.Profig(key).get().map(_.asString).map(_.trim).filter(_.nonEmpty)

      def parseChildParentFields(s: String): Map[String, String] = {
        s.split(",").toList.flatMap { pair =>
          val i = pair.indexOf(':')
          if i <= 0 || i >= pair.length - 1 then Nil
          else {
            val child = pair.substring(0, i).trim
            val field = pair.substring(i + 1).trim
            if child.isEmpty || field.isEmpty then Nil else List(child -> field)
          }
        }.toMap
      }

      def candidates: List[Candidate] =
        lightDB.stores.flatMap { s =>
          s.model match {
            case pcs: ParentChildSupport[?, ?, ?] @unchecked =>
              val parentStoreName = s.name
              try {
                val explicitParent = hasExplicitJoinKeys(parentStoreName)
                val childStoreName = pcs.childStoreName
                val childParentFieldName = if explicitParent then {
                  val mapping = profigString(s"lightdb.opensearch.$parentStoreName.joinChildParentFields")
                    .map(parseChildParentFields)
                    .getOrElse(Map.empty)
                  mapping.getOrElse(childStoreName, pcs.childJoinParentFieldName)
                } else {
                  pcs.childJoinParentFieldName
                }
                val joinDomain = if explicitParent then {
                  profigString(s"lightdb.opensearch.$parentStoreName.joinDomain")
                    .getOrElse(pcs.joinDomainName(parentStoreName))
                } else {
                  pcs.joinDomainName(parentStoreName)
                }
                val joinFieldName = if explicitParent then {
                  profigString("lightdb.opensearch.joinFieldName").getOrElse(pcs.joinFieldName)
                } else {
                  pcs.joinFieldName
                }
                List(Candidate(parentStoreName, joinDomain, joinFieldName, childStoreName, childParentFieldName))
              } catch {
                case _: Throwable =>
                  Nil
              }
            case _ =>
              Nil
          }
        }

      val directParent = candidates.find(_.parentStoreName == name)
      val asChild = candidates.filter(_.childStoreName == name)

      (directParent, asChild) match {
        case (Some(c), _) =>
          OpenSearchJoinDomainRegistry.register(
            dbName = lightDB.name,
            joinDomain = c.joinDomain,
            parentStoreName = c.parentStoreName,
            childJoinParentFields = Map(c.childStoreName -> c.childParentFieldName),
            joinFieldName = c.joinFieldName
          )
        case (None, one :: Nil) =>
          OpenSearchJoinDomainRegistry.register(
            dbName = lightDB.name,
            joinDomain = one.joinDomain,
            parentStoreName = one.parentStoreName,
            childJoinParentFields = Map(one.childStoreName -> one.childParentFieldName),
            joinFieldName = one.joinFieldName
          )
        case (None, Nil) =>
          ()
        case (None, many) =>
          throw new IllegalArgumentException(
            s"OpenSearch auto join-domain configuration for '$name' is ambiguous. Multiple ParentChildSupport parents claim this child: " +
              many.map(_.parentStoreName).distinct.sorted.mkString(", ")
          )
      }
      }
    }
  }

  private lazy val config = {
    ensureAutoJoinDomainRegistry()
    OpenSearchConfig.from(lightDB, name)
  }
  private lazy val client = OpenSearchClient(config)

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
    if config.useIndexAlias && config.useWriteAlias then writeAliasName
    else indexAliasName
  }

  // Backwards-compatible name used throughout the existing code: treat as the read target.
  lazy val indexName: String = readIndexName

  // Ensure the OpenSearch index (and optional aliases) exist before the store is used.
  // This is intentionally independent of transactions so it can be safely invoked from createTransaction to avoid
  // startup races (db.init may be started but not completed when the first request arrives).
  private lazy val ensureIndexReady: Task[Unit] = Task.defer {
    // In a join domain, only the join-parent should create/own the physical index mapping.
    // Child collections share the same index and can safely skip index creation during initialization.
    val isJoinChild: Boolean = config.joinDomain.nonEmpty && config.joinRole.contains("child")

    def checkMappingFor(targetIndex: String): Task[Unit] = {
      val warn = config.mappingHashWarnOnly
      if isJoinChild || config.ignoreMappingHash then {
        Task.unit
      } else {
        val expectedBody = OpenSearchTemplates.indexBody(model, fields, config, name, maxResultWindow = config.maxResultWindow)
        val expected = expectedBody
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
                val msg =
                  s"""OpenSearch mapping hash mismatch for '$targetIndex' (expected=$exp actual=$actual).
                     |This usually means the DocumentModel/indexed fields changed incompatibly.
                     |Create a new physical index and swap the read/write alias (recommended), or set
                     |'lightdb.opensearch.ignoreMappingHash=true' to bypass this check.""".stripMargin.replace("\n", " ")
                val canAutoMigrate =
                  config.mappingHashAutoMigrate &&
                    config.useIndexAlias && // safe migration requires aliases
                    targetIndex != indexAliasName // if alias name is a real index, we can't safely alias-swap
                if canAutoMigrate then {
                  logger.warn(s"$msg Auto-migrating via alias reindex+swap (mappingHashAutoMigrate=true)...")
                  val writeAliasOpt = if config.useWriteAlias then Some(writeAliasName) else None
                  OpenSearchIndexMigration
                    .reindexAndRepointAliases(
                      client = client,
                      readAlias = indexAliasName,
                      writeAlias = writeAliasOpt,
                      newIndexBody = expectedBody,
                      defaultSuffix = config.indexAliasSuffix
                    )
                    .flatMap { newIndex =>
                      logger.info(s"OpenSearch alias migration complete for '$indexAliasName' -> '$newIndex'")
                    }
                } else if warn then {
                  logger.warn(msg)
                } else {
                  Task.error(new RuntimeException(msg))
                }
              case None =>
                // Should not happen: our template always includes mapping_hash.
                Task.unit
            }
        }
      }
    }

    def isAlreadyExists(t: Throwable): Boolean = {
      val msg = Option(t.getMessage).getOrElse("").toLowerCase
      msg.contains("resource_already_exists_exception") || msg.contains("already exists")
    }

    def createIndexIdempotent(index: String, body: fabric.Json): Task[Unit] =
      client.createIndex(index, body).attempt.flatMap {
        case scala.util.Success(_) => Task.unit
        case scala.util.Failure(t) if isAlreadyExists(t) =>
          // Another node/process (or a concurrent transaction) created it first.
          Task.unit
        case scala.util.Failure(t) =>
          Task.error(t)
      }

    if config.useIndexAlias then {
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
          if isJoinChild then {
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
                    createIndexIdempotent(physicalIndexName, body)
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
          if isJoinChild then {
            Task.unit
          } else {
            // Minimal v1 index creation:
            // - set max_result_window high enough for LightDB offset-based streaming in tests/specs
            // - explicit mappings for indexed fields (strings/numbers/geo centers) to avoid sort/runtime 400s
            val body = OpenSearchTemplates.indexBody(model, fields, config, name, maxResultWindow = config.maxResultWindow)
            createIndexIdempotent(indexName, body).next(checkMappingFor(indexName))
          }
      }
    }
  }.singleton.unit

  override protected def initialize(): Task[Unit] =
    super.initialize().next(ensureIndexReady)

  private def createAliasesForPhysicalIndex(): Task[Unit] = {
    val actions = if config.useWriteAlias then {
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
    if !config.useWriteAlias then {
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

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => WriteHandler[Doc, Model]): Task[TX] =
    ensureIndexReady.next(Task(OpenSearchTransaction(this, config, client, parent, writeHandlerFactory)))
}

object OpenSearchStore extends CollectionManager with PrefixScanningStoreManager {
  /**
   * Threshold before task-capable APIs (e.g., _delete_by_query, _reindex) switch to async polling.
   * Set to <= 0 to disable async polling fallback.
   */
  @volatile var asyncTaskPollAfter: FiniteDuration = 10.seconds

  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = OpenSearchStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] = {
    new OpenSearchStore[Doc, Model](
      name = name,
      // OpenSearch is an external service; there is no local on-disk store directory for this collection.
      // Keep path=None to avoid implying filesystem persistence and to avoid creating unused directories in tests.
      path = None,
      model = model,
      storeMode = storeMode,
      lightDB = db,
      storeManager = this
    )
  }
}


