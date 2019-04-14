package akka.http.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait Boot {

  implicit val system: ActorSystem

  implicit val materializer: ActorMaterializer

  implicit val executionContext: ExecutionContext

}

object MainConfig {

  val TotalCount: Int = 1024 * 1024

  val MaxOpenRequests = 1024

  val MaxConnections = 1024

}

trait MainTest {

  self: Boot =>



  val url = "http://127.0.0.1/health"

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

    totalResponseCount.getAndIncrement()

    if(totalResponseCount.get() == MainConfig.TotalCount) {
      endTime = System.currentTimeMillis()

      val elapsed = endTime - startTime

      println(s"Elapsed: $elapsed ms")

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
    akka.stream.materializer.debug.fuzzing-mode = off
    akka.http.host-connection-pool.max-connections = ${MainConfig.MaxConnections}
    akka.http.host-connection-pool.max-open-requests = ${MainConfig.MaxOpenRequests}
    """)


  override implicit val system: ActorSystem = ActorSystem("ServerTest", testConf)
  override implicit val materializer: ActorMaterializer = ActorMaterializer()
  override implicit val executionContext: ExecutionContext = system.dispatcher

}

object Main extends App {

//  val default = new MainDefaultConnectionPool {}
//  default.bootstrap()
//  111764 ms

  val enhanced = new MainEnhancedConnectionPool {}
  enhanced.bootstrap()

}
