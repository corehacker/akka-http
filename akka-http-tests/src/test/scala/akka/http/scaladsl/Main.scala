package akka.http.scaladsl

import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait Boot {

  implicit val system: ActorSystem

  implicit val materializer: ActorMaterializer

  implicit val executionContext: ExecutionContext

}

object MainConfig {

  val TotalCount: Int = 1024*256

  val MaxOpenRequests: Int = 512

  val MaxConnections: Int = 512

}

trait MainTest {

  self: Boot =>



  val url = "http://127.0.0.1/"

  val totalRequestCount = new AtomicInteger(0)
  val totalResponseCount = new AtomicInteger(0)

  val outstandingRequestCount = new AtomicInteger(0)

  var startTime: Long = 0
  var endTime: Long = 0

  private def handleResponse(res: Option[HttpResponse] = None): Unit = {

    res match {
      case Some(value) =>
        value.discardEntityBytes()

//        value.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).map(_.utf8String).map { s =>
//          println(" ------------ RESPONSE ------------")
//          println(s)
//          println(" -------- END OF RESPONSE ---------")
//        }

      case None =>
    }

    outstandingRequestCount.getAndDecrement()

    val count = totalResponseCount.getAndIncrement()

    if(count > 0 && count % 10000 == 0) {
      endTime = System.currentTimeMillis()

      val elapsed = endTime - startTime
      val perSecond: Float = count / (elapsed / 1000)
      println(s"${Timestamp.from(Instant.now)} - Processed $count responses. (${elapsed}ms @ $perSecond req/s)")
    }

    if(totalResponseCount.get() == MainConfig.TotalCount) {
      endTime = System.currentTimeMillis()

      val elapsed = endTime - startTime

      println("Requested Count: " + totalRequestCount.get())
      println("Responses Count: " + totalResponseCount.get())
      println(s"${Timestamp.from(Instant.now)} - Elapsed: $elapsed ms")

      system.terminate()
    }

    sendRequest()

  }

  private def sendRequest(): Unit = {
    if(totalRequestCount.get() != MainConfig.TotalCount) {

      outstandingRequestCount.getAndIncrement()

      totalRequestCount.getAndIncrement()

      val x = Http().singleRequest(HttpRequest(uri = url))
      x.onComplete {
        case Failure(_) => handleResponse()
        case Success(res: HttpResponse) => handleResponse(Option(res))
      }

    }
  }

  def bootstrap(): Unit = {
    startTime = System.currentTimeMillis()
    while(outstandingRequestCount.get() < MainConfig.MaxOpenRequests) {
      sendRequest()
    }
  }

}

trait MainEnhancedConnectionPool extends MainTest with Boot {

  val testConf: Config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    akka.log-dead-letters = off
    akka.actor.default-dispatcher.default-executor.fallback = "thread-pool-executor"
    akka.actor.default-dispatcher.thread-pool-executor.core-pool-size-max = 8
    akka.actor.default-dispatcher.thread-pool-executor.core-pool-size-factor = 1
    akka.stream.materializer.debug.fuzzing-mode = off
    akka.http.host-connection-pool.pool-implementation = enhanced
    akka.http.host-connection-pool.max-connections = ${MainConfig.MaxConnections}
    akka.http.host-connection-pool.max-open-requests = ${MainConfig.MaxOpenRequests}
    """)

  override implicit val system: ActorSystem = ActorSystem("ServerTest", testConf)
  override implicit val materializer: ActorMaterializer = ActorMaterializer()
  override implicit val executionContext: ExecutionContext = system.dispatcher


}

trait MainDefaultConnectionPool extends MainTest with Boot {

  val testConf: Config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    akka.log-dead-letters = off
    akka.io.tcp.nr-of-selectors = 4
    akka.stream.materializer.debug.fuzzing-mode = off
    akka.http.host-connection-pool.pool-implementation = new
    akka.http.host-connection-pool.max-connections = ${MainConfig.MaxConnections}
    akka.http.host-connection-pool.max-open-requests = ${MainConfig.MaxOpenRequests}
    """)


  override implicit val system: ActorSystem = ActorSystem("ServerTest", testConf)
  override implicit val materializer: ActorMaterializer = ActorMaterializer()
  override implicit val executionContext: ExecutionContext = system.dispatcher

}

object Main extends App {

  sys.env.getOrElse("CONNECTION_POOL_NAME", "new") match {
    case "new" =>
      println("Running default connection pool...")
      val default = new MainDefaultConnectionPool {}
      default.bootstrap()

      // MaxOpenRequests - 512
      // MaxConnections - 512
      // 1m requests - 555901ms
    case "enhanced" =>
      println("Running enhanced connection pool...")
      val enhanced = new MainEnhancedConnectionPool {}
      enhanced.bootstrap()

      // MaxOpenRequests - 512
      // MaxConnections - 512
      // 1m requests - 568119ms
  }

}
