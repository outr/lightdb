package lightdb.sql.connect

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.sql.{CallableStatement, Connection, PreparedStatement, Statement}
import scala.collection.mutable

/**
 * A connection that opens its database transaction only when the first statement that writes or locks runs.
 *
 * A pooled connection in manual-commit mode starts a transaction with its first statement, read or not, so a
 * transaction that only read still ends in a COMMIT: a round trip that changes nothing. Under READ COMMITTED (the
 * PostgreSQL default) every statement takes its own snapshot whether or not a transaction is open, so a read run in
 * auto-commit mode sees exactly what it would have seen inside one. The wrapper therefore keeps the physical
 * connection in auto-commit until a statement that is not a plain read executes, and switches it to manual commit just
 * before that statement, so the write and everything after it form the transaction the caller commits. After a
 * commit or rollback the next transaction starts lazily again.
 *
 * To the caller the connection is in the mode it asked for: `getAutoCommit` is false in manual mode, and `commit` /
 * `rollback` with no transaction open do nothing.
 *
 * What opens the transaction:
 *  - any statement that is not a plain read (see [[readsOnly]]), including `executeUpdate`, batches and calls;
 *  - a read that locks rows (`FOR UPDATE` / `FOR SHARE`): its lock must live until the caller's commit;
 *  - a read whose result may be larger than its fetch size: a driver streams a result through a server-side cursor
 *    only inside a transaction (in auto-commit PostgreSQL fetches the whole result at once), so an unbounded read
 *    runs in the transaction to keep its memory bounded. A read is bounded when its statement has no fetch size, its
 *    top-level `LIMIT` (a literal or a bound parameter) is at most the fetch size, or it is a `SELECT COUNT(...)`;
 *  - a savepoint, and unwrapping to the driver's own connection (whatever runs on it bypasses this wrapper).
 *
 * Only for READ COMMITTED or weaker isolation: under REPEATABLE READ or SERIALIZABLE the reads of one transaction
 * share a snapshot, which reads run in auto-commit would not. [[DataSourceConnectionManager]] checks the isolation
 * level before wrapping.
 */
object LazyBeginConnection {
  private val ReadStarts = List("select", "with", "values", "show", "explain", "table")
  private val WriteMarks =
    """\b(insert|update|delete|merge|create|alter|drop|truncate|copy|call|do|grant|revoke|vacuum|analyze|lock|set|nextval|setval|pg_advisory\w*)\b|\bfor\s+(update|share|no\s+key|key\s+share)\b""".r
  private val CountStart = """^select\s+count\s*\(""".r
  private val LimitAt = """^limit\s+(\?|\d+)""".r

  /** Whether `sql` only reads: it starts as a query and mentions nothing that writes or locks. Anything unsure is a
    * write, which costs only the commit this wrapper exists to save. */
  def readsOnly(sql: String): Boolean = {
    val s = normalize(sql)
    ReadStarts.exists(s.startsWith) && WriteMarks.findFirstIn(s).isEmpty
  }

  /** Whether the read `sql` returns at most `fetchSize` rows, given the integer parameters bound so far (1-based). */
  def boundedRead(sql: String, fetchSize: Int, maxRows: Int, parameters: collection.Map[Int, Long]): Boolean =
    fetchSize <= 0 || (maxRows > 0 && maxRows <= fetchSize) ||
      CountStart.findPrefixOf(normalize(sql)).nonEmpty || topLevelLimit(sql.toLowerCase, parameters).exists(_ <= fetchSize)

  private def normalize(sql: String): String = sql.dropWhile(c => c.isWhitespace || c == '(').toLowerCase

  // The value of the LIMIT outside every parenthesis (a subquery's LIMIT bounds nothing outside it), counting the
  // `?` placeholders before it to find its parameter. Quoted text is skipped; `??` is a driver escape, not a
  // placeholder.
  private def topLevelLimit(s: String, parameters: collection.Map[Int, Long]): Option[Long] = {
    var depth = 0
    var placeholders = 0
    var quote: Char = 0
    var found: Option[Long] = None
    var i = 0
    while i < s.length do {
      val c = s.charAt(i)
      if quote != 0 then {
        if c == quote then quote = 0
      } else c match {
        case '\'' | '"' => quote = c
        case '(' => depth += 1
        case ')' => depth -= 1
        case '?' =>
          if i + 1 < s.length && s.charAt(i + 1) == '?' then i += 1
          else placeholders += 1
        case 'l' if depth == 0 && (i == 0 || !Character.isLetterOrDigit(s.charAt(i - 1))) =>
          LimitAt.findPrefixMatchOf(s.substring(i)).foreach { m =>
            found = m.group(1) match {
              case "?" => parameters.get(placeholders + 1)
              case digits => digits.toLongOption
            }
          }
        case _ => ()
      }
      i += 1
    }
    found
  }

  /** `raw`, wrapped so that its transactions begin lazily, when it is handed out in manual-commit mode. */
  def apply(raw: Connection): Connection =
    if raw.getAutoCommit then raw
    else {
      raw.setAutoCommit(true)
      new LazyBeginConnection(raw).proxy
    }
}

private final class LazyBeginConnection(raw: Connection) {
  // The mode the caller asked for: manual commit, until it says otherwise.
  @volatile private var manual = true

  private def call(target: AnyRef, method: Method, args: Array[AnyRef]): AnyRef =
    try method.invoke(target, (if args == null then Array.empty[AnyRef] else args)*)
    catch { case e: InvocationTargetException => throw e.getCause }

  private def open: Boolean = manual && !raw.getAutoCommit

  /** Open the transaction now, if the caller is in manual mode and none is open. */
  private def begin(): Unit = if manual && raw.getAutoCommit then raw.setAutoCommit(false)

  // After the transaction ends the next one begins lazily again.
  private def ended(): Unit = if manual && !raw.isClosed && !raw.getAutoCommit then raw.setAutoCommit(true)

  private def statement(target: Statement, iface: Class[?], preparedSql: Option[String]): AnyRef = {
    val parameters = mutable.Map.empty[Int, Long]
    Proxy.newProxyInstance(getClass.getClassLoader, Array[Class[?]](iface), new InvocationHandler {
      override def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = method.getName match {
        case "unwrap" if args != null && args(0).asInstanceOf[Class[?]].isInstance(proxy) => proxy
        case "isWrapperFor" if args != null && args(0).asInstanceOf[Class[?]].isInstance(proxy) => java.lang.Boolean.TRUE
        case "getConnection" => LazyBeginConnection.this.proxy
        // The integer parameters bound by position, for a LIMIT given as a parameter.
        case name if name.startsWith("set") && args != null && args.length >= 2 && args(0).isInstanceOf[java.lang.Integer] =>
          val index = args(0).asInstanceOf[java.lang.Integer].intValue()
          args(1) match {
            case n: java.lang.Number if name != "setNull" => parameters.put(index, n.longValue())
            case _ => parameters.remove(index)
          }
          call(target, method, args)
        case "clearParameters" =>
          parameters.clear()
          call(target, method, args)
        case "executeQuery" | "execute" =>
          val sql = preparedSql.orElse(Option(args).flatMap(_.headOption).collect { case s: String => s })
          val read = sql.exists(s => LazyBeginConnection.readsOnly(s) &&
            LazyBeginConnection.boundedRead(s, target.getFetchSize, target.getMaxRows, parameters))
          if !read then begin()
          call(target, method, args)
        case name if name.startsWith("execute") || name == "addBatch" =>
          begin()
          call(target, method, args)
        case _ => call(target, method, args)
      }
    })
  }

  lazy val proxy: Connection = Proxy.newProxyInstance(getClass.getClassLoader, Array[Class[?]](classOf[Connection]), new InvocationHandler {
    override def invoke(p: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = method.getName match {
      case "unwrap" if args != null && args(0).asInstanceOf[Class[?]].isInstance(p) => p
      case "isWrapperFor" if args != null && args(0).asInstanceOf[Class[?]].isInstance(p) => java.lang.Boolean.TRUE
      case "unwrap" =>
        begin()
        call(raw, method, args)
      case "getAutoCommit" => java.lang.Boolean.valueOf(!manual)
      case "setAutoCommit" =>
        val on = args(0).asInstanceOf[java.lang.Boolean].booleanValue()
        if on then {
          // JDBC: turning auto-commit on commits the open transaction.
          raw.setAutoCommit(true)
          manual = false
        } else manual = true
        null
      case "commit" =>
        if open then {
          raw.commit()
          ended()
        }
        null
      case "rollback" if args == null || args.isEmpty =>
        if open then {
          raw.rollback()
          ended()
        }
        null
      case "setSavepoint" =>
        begin()
        call(raw, method, args)
      case "prepareStatement" =>
        statement(call(raw, method, args).asInstanceOf[Statement], classOf[PreparedStatement], Some(args(0).asInstanceOf[String]))
      case "prepareCall" =>
        statement(call(raw, method, args).asInstanceOf[Statement], classOf[CallableStatement], None)
      case "createStatement" =>
        statement(call(raw, method, args).asInstanceOf[Statement], classOf[Statement], None)
      case "close" =>
        // Hand the connection back in the mode the pool gave it out in.
        if manual && !raw.isClosed && raw.getAutoCommit then raw.setAutoCommit(false)
        call(raw, method, args)
      case _ => call(raw, method, args)
    }
  }).asInstanceOf[Connection]
}
