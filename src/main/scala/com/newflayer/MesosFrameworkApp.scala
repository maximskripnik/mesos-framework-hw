package com.newflayer

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object MesosFrameworkApp {

  def apply(mesosAddress: String, scriptUrl: String, scriptArgs: List[String]): Behavior[NotUsed] =
    Behaviors.setup { context =>
      implicit val ec = context.executionContext

      val mesosActor = context.spawn(
        MesosFramework(Http()(context.system.toClassic), mesosAddress)(context.executionContext),
        "mesos-framework"
      )
      val mesosFramework = new MesosFramework(mesosActor)(context.system.scheduler)

      val resultF = for {
        _ <- mesosFramework.start()
        _ <- mesosFramework.executeTask(TaskType.SimpleDockerContainer)
        _ <- mesosFramework.executeTask(
          TaskType
            .BashScript(scriptUrl = scriptUrl, arguments = scriptArgs)
        )
        _ <- mesosFramework.stop()
      } yield ()

      Await.result(resultF, Duration.Inf)

      Behaviors.stopped
    }

  def main(args: Array[String]): Unit = {
    val (mesosAddress, scriptUrl, scriptArgs) = args.toList match {
      case mesosAddress :: scriptUrl :: scriptArgs => (mesosAddress, scriptUrl, scriptArgs)
      case _ =>
        throw new RuntimeException(
          "Bad args. Usage: com.newflayer.MesosFrameworkApp <mesosAddress> <scriptUrl> <scriptArgs*>"
        )
    }
    ActorSystem(MesosFrameworkApp(mesosAddress, scriptUrl, scriptArgs), "main")
  }

}
