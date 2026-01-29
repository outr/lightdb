package lightdb.h2

import fabric.io.JsonParser
import fabric.{Arr, Bool, Json, Null, NumDec, NumInt, Str}
import org.h2.api.Trigger

import java.sql.{Connection, PreparedStatement}

/**
 * Maintains auxiliary multi-value index tables for array-like fields (List/Set) stored as JSON arrays.
 *
 * Trigger naming convention (created by H2Store):
 *   <baseTable>__mv__<fieldName>__ai / __au / __ad
 *
 * The auxiliary table name is:
 *   <baseTable>__mv__<fieldName>
 *
 * Schema: PUBLIC
 */
class H2MultiValueTrigger extends Trigger {
  private var schema: String = _
  private var baseTable: String = _
  private var fieldName: String = _
  private var mvTable: String = _
  private var idIndex: Int = -1
  private var fieldIndex: Int = -1

  override def init(conn: Connection,
                    schemaName: String,
                    triggerName: String,
                    tableName: String,
                    before: Boolean,
                    `type`: Int): Unit = {
    schema = schemaName
    baseTable = tableName

    // Parse fieldName from triggerName: <table>__mv__<field>__ai
    val marker = "__mv__"
    val lower = triggerName.toLowerCase
    val idx = lower.indexOf(marker)
    if idx == -1 then throw new IllegalStateException(s"Unexpected trigger name: $triggerName")
    val after = triggerName.substring(idx + marker.length)
    val field = after.split("__").headOption.getOrElse(throw new IllegalStateException(s"Unable to parse field from: $triggerName"))
    fieldName = field
    mvTable = s"${baseTable}__mv__${fieldName}"

    // Determine column positions (in row arrays) by reading metadata in column order.
    // H2 provides COLUMN_ID as 1-based ordinal.
    val cols = conn.getMetaData.getColumns(null, schemaName, tableName, null)
    try {
      while cols.next() do {
        val colName = cols.getString("COLUMN_NAME")
        val ordinal = cols.getInt("ORDINAL_POSITION") - 1
        if colName.equalsIgnoreCase("_id") then idIndex = ordinal
        if colName.equalsIgnoreCase(fieldName) then fieldIndex = ordinal
      }
    } finally {
      cols.close()
    }

    if idIndex < 0 then throw new IllegalStateException(s"Unable to find _id column index for $schemaName.$tableName")
    if fieldIndex < 0 then throw new IllegalStateException(s"Unable to find '$fieldName' column index for $schemaName.$tableName")
  }

  override def fire(conn: Connection, oldRow: Array[AnyRef], newRow: Array[AnyRef]): Unit = {
    val ownerId: String =
      if newRow != null then Option(newRow(idIndex)).map(_.toString).orNull
      else Option(oldRow(idIndex)).map(_.toString).orNull

    if ownerId == null then ()
    else {
      deleteOwner(conn, ownerId)

      // INSERT/UPDATE: re-add from new value
      if newRow != null then {
        val jsonStr = Option(newRow(fieldIndex)).map(_.toString).orNull
        if jsonStr != null && jsonStr.nonEmpty then {
          valuesFromJson(jsonStr).foreach { v =>
            insertValue(conn, ownerId, v)
          }
        }
      }
    }
  }

  private def deleteOwner(conn: Connection, ownerId: String): Unit = {
    val ps = conn.prepareStatement(s"DELETE FROM $mvTable WHERE owner_id = ?")
    try {
      ps.setString(1, ownerId)
      ps.executeUpdate()
    } finally {
      ps.close()
    }
  }

  private def insertValue(conn: Connection, ownerId: String, value: String): Unit = {
    val ps: PreparedStatement = conn.prepareStatement(s"INSERT INTO $mvTable(owner_id, value) VALUES(?, ?)")
    try {
      ps.setString(1, ownerId)
      ps.setString(2, value)
      ps.executeUpdate()
    } finally {
      ps.close()
    }
  }

  private def valuesFromJson(jsonStr: String): List[String] = {
    val json = try {
      Some(JsonParser(jsonStr))
    } catch {
      case _: Throwable =>
        // Not JSON array, treat as one string
        None
    }
    json match {
      case Some(Arr(vector, _)) =>
        vector.toList.flatMap(v => scalarValue(v))
      case Some(other) =>
        scalarValue(other).toList
      case None =>
        List(jsonStr)
    }
  }

  private def scalarValue(json: Json): Option[String] = json match {
    case Null => None
    case Str(s, _) => Some(s)
    case NumInt(l, _) => Some(l.toString)
    case NumDec(bd, _) => Some(bd.toString)
    case Bool(b, _) => Some(if b then "1" else "0")
    case other => Some(other.toString)
  }

  override def close(): Unit = ()
  override def remove(): Unit = ()
}


