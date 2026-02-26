package lightdb.googlesheets

import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.model.*
import fabric.io.{JsonFormatter, JsonParser}
import fabric.{Json, Null, Obj, Str, NumInt, NumDec, Bool, Arr}
import lightdb.id.Id
import rapid.Task

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

class GoogleSheetsInstance(spreadsheetId: String, sheetName: String, service: Sheets) {
  private val cache = new ConcurrentHashMap[String, (Int, Json)]()
  private var headers: List[String] = Nil
  private var headerDirty: Boolean = false
  private var nextRow: Int = 2 // row 1 is header

  // Write buffering: track dirty IDs and pending row clears
  private val dirtyIds = ConcurrentHashMap.newKeySet[String]()
  private val pendingClears = new ConcurrentLinkedQueue[Integer]()

  init()

  private def init(): Unit = {
    // Ensure sheet tab exists; remove default "Sheet1" if present
    val spreadsheet = service.spreadsheets().get(spreadsheetId).execute()
    val sheets = spreadsheet.getSheets.asScala.toList
    val sheetExists = sheets.exists(_.getProperties.getTitle == sheetName)
    if (!sheetExists) {
      val requests = new java.util.ArrayList[Request]()
      requests.add(new Request().setAddSheet(new AddSheetRequest().setProperties(new SheetProperties().setTitle(sheetName))))
      // Delete empty default "Sheet1" if it exists alongside other sheets
      if (sheets.size >= 1) {
        sheets.find(_.getProperties.getTitle == "Sheet1").foreach { sheet1 =>
          requests.add(new Request().setDeleteSheet(new DeleteSheetRequest().setSheetId(sheet1.getProperties.getSheetId)))
        }
      }
      val request = new BatchUpdateSpreadsheetRequest().setRequests(requests)
      service.spreadsheets().batchUpdate(spreadsheetId, request).execute()
    }

    // Read entire sheet into cache
    val range = s"'$sheetName'"
    try {
      val response = service.spreadsheets().values().get(spreadsheetId, range).execute()
      val rows = Option(response.getValues).map(_.asScala.toList).getOrElse(Nil)
      rows match {
        case headerRow :: dataRows =>
          headers = headerRow.asScala.toList.map(_.toString)
          var rowNum = 2
          for (row <- dataRows) {
            val cells = row.asScala.toList.map(_.toString)
            if (cells.nonEmpty && cells.head.nonEmpty) {
              val id = cells.head
              val json = cellsToJson(headers, cells)
              cache.put(id, (rowNum, json))
            }
            rowNum += 1
          }
          nextRow = rowNum
        case Nil =>
          nextRow = 2
      }
    } catch {
      case _: com.google.api.client.googleapis.json.GoogleJsonResponseException =>
        nextRow = 2
    }
  }

  def put(id: String, json: Json): Task[Unit] = Task {
    synchronized {
      val jsonObj = json match {
        case o: Obj => o.value.toMap
        case _ => Map("_id" -> json)
      }

      if (headers.isEmpty) {
        headers = "_id" :: jsonObj.keys.filter(_ != "_id").toList
        headerDirty = true
      } else {
        val newKeys = jsonObj.keys.filter(k => k != "_id" && !headers.contains(k)).toList
        if (newKeys.nonEmpty) {
          headers = headers ++ newKeys
          headerDirty = true
        }
      }

      val existing = cache.get(id)
      val rowNum = if (existing != null) existing._1 else { val r = nextRow; nextRow += 1; r }
      cache.put(id, (rowNum, json))
      dirtyIds.add(id)
    }
  }

  /** Flush all pending writes to the sheet in a single batchUpdate API call. */
  def flush(): Task[Unit] = Task {
    synchronized {
      val writes = new java.util.ArrayList[ValueRange]()

      // Header row
      if (headerDirty) {
        val headerCells = headers.map(_.asInstanceOf[AnyRef]).asJava
        writes.add(new ValueRange().setRange(s"'$sheetName'!A1").setValues(java.util.List.of(headerCells)))
        headerDirty = false
      }

      // Dirty data rows
      dirtyIds.forEach { id =>
        val entry = cache.get(id)
        if (entry != null) {
          val (rowNum, json) = entry
          val cells = jsonToCells(headers, json)
          writes.add(new ValueRange()
            .setRange(s"'$sheetName'!A$rowNum")
            .setValues(java.util.List.of(cells.map(_.asInstanceOf[AnyRef]).asJava)))
        }
      }
      dirtyIds.clear()

      // Pending row clears (from deletes)
      val numCols = if (headers.nonEmpty) headers.size else 1
      var rowToClear: Integer = pendingClears.poll()
      while (rowToClear != null) {
        val emptyCells = Collections.nCopies(numCols, "".asInstanceOf[AnyRef])
        writes.add(new ValueRange()
          .setRange(s"'$sheetName'!A$rowToClear")
          .setValues(java.util.List.of(emptyCells)))
        rowToClear = pendingClears.poll()
      }

      if (!writes.isEmpty) {
        val body = new BatchUpdateValuesRequest()
          .setValueInputOption("RAW")
          .setData(writes)
        service.spreadsheets().values().batchUpdate(spreadsheetId, body).execute()
      }
    }
  }

  def get(id: String): Task[Option[Json]] = Task {
    Option(cache.get(id)).map(_._2)
  }

  def exists(id: String): Task[Boolean] = Task {
    cache.containsKey(id)
  }

  def count: Task[Int] = Task {
    cache.size()
  }

  def stream: rapid.Stream[Json] = rapid.Stream
    .fromIterator(Task(cache.values().iterator().asScala.map(_._2)))

  def delete(id: String): Task[Unit] = Task {
    synchronized {
      val existing = cache.remove(id)
      if (existing != null) {
        dirtyIds.remove(id)
        pendingClears.add(existing._1)
      }
    }
  }

  def truncate(): Task[Int] = flush().map { _ =>
    synchronized {
      val size = cache.size()
      if (size > 0 || nextRow > 2) {
        val range = s"'$sheetName'!A2:ZZ"
        val body = new ClearValuesRequest()
        service.spreadsheets().values().clear(spreadsheetId, range, body).execute()
        cache.clear()
        nextRow = 2
      }
      size
    }
  }

  def dispose(): Task[Unit] = flush()

  // --- Column mapping ---

  private def jsonToCells(headers: List[String], json: Json): List[String] = {
    val obj = json match {
      case o: Obj => o.value.toMap
      case _ => Map.empty[String, Json]
    }
    headers.map { key =>
      obj.get(key) match {
        case None => ""
        case Some(Null) => ""
        case Some(Str(s, _)) => s
        case Some(NumInt(n, _)) => n.toString
        case Some(NumDec(n, _)) => n.toString
        case Some(Bool(b, _)) => b.toString
        case Some(other) => JsonFormatter.Compact(other)
      }
    }
  }

  private def cellsToJson(headers: List[String], cells: List[String]): Json = {
    val pairs = headers.zipWithIndex.map { case (key, idx) =>
      val cellValue = if (idx < cells.size) cells(idx) else ""
      val jsonValue = if (cellValue.isEmpty) {
        Null
      } else {
        try {
          JsonParser(cellValue) match {
            case s: Str => s
            case parsed => parsed
          }
        } catch {
          case _: Exception => Str(cellValue)
        }
      }
      key -> jsonValue
    }
    Obj(pairs.toMap)
  }
}
