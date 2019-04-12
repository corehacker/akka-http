package akka.http.scaladsl

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.Await
import scala.concurrent.duration._

object Main extends App {

  val testConf: Config = ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.log-dead-letters = off
    akka.stream.materializer.debug.fuzzing-mode = off
    akka.http.host-connection-pool.pool-implementation = enhanced
    """)
  implicit val system = ActorSystem("ServerTest", testConf)
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  import system.dispatcher

  val url = "http://127.0.0.1/health"

  val x = Http().singleRequest(HttpRequest(uri = url))

  val res = Await.result(x, 10.seconds)

  val response = res.entity.dataBytes.runFold(ByteString.empty)(_ ++ _).map(_.utf8String)

  println(" ------------ RESPONSE ------------")
  println(Await.result(response, 10.seconds))
  println(" -------- END OF RESPONSE ---------")

  system.terminate()

}
