package spec

import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import rapid.Task

import java.net.ServerSocket
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

@EmbeddedTest
class OpenSearchClientHardCapSpec extends AnyWordSpec with Matchers {
  "OpenSearchClient sendWithRetry hard cap" should {
    "fail boundedly when transport never returns a response" in {
      val server = new ServerSocket(0)
      val port = server.getLocalPort
      val acceptThread = new Thread(new Runnable {
        override def run(): Unit = {
        val socket = server.accept()
        // Keep connection open long enough that requestTimeout (10s) would not fire first.
        Thread.sleep(10_000L)
        Try(socket.close())
        }
      })
      acceptThread.setDaemon(true)
      acceptThread.start()

      val propKey = "lightdb.opensearch.hardCapMs"
      val previous = sys.props.get(propKey)
      sys.props.put(propKey, "1200")

      val client = OpenSearchClient(
        OpenSearchConfig(
          baseUrl = s"http://127.0.0.1:$port",
          requestTimeout = 10.seconds,
          slowRequestLogAfter = None,
          slowRequestLogEvery = None,
          retryMaxAttempts = 1,
          logRequests = true
        )
      )

      val started = System.currentTimeMillis()
      client
        .indexExists("hard-cap-test")
        .attempt
        .guarantee(Task {
          previous match {
            case Some(v) => sys.props.put(propKey, v)
            case None => sys.props.remove(propKey)
          }
          Try(server.close())
        }.unit)
        .map {
          case Failure(t) =>
            val elapsedMs = System.currentTimeMillis() - started
            elapsedMs should be < 6000L
            t.getMessage.toLowerCase should include("hard cap")
            succeed
          case Success(value) =>
            fail(s"Expected hard-cap failure, but got success: $value")
        }
    }
  }
}

