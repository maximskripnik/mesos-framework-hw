package com.newflayer

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.Scheduler
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.ContentType
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.MediaType
import akka.http.scaladsl.model.MediaType.Compressible
import akka.http.scaladsl.model.headers
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.alpakka.recordio.scaladsl.RecordIOFraming
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Call.Subscribe
import org.apache.mesos.v1.scheduler.Protos.Event
import org.slf4j.Logger
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Success

class MesosFramework(
  mesosActor: ActorRef[MesosFramework.Command]
)(
  implicit scheduler: Scheduler
) {

  def start(): Future[Unit] = mesosActor.ask(ref => MesosFramework.Start(ref))(5.seconds, scheduler)

  def executeTask(taskType: TaskType): Future[Unit] =
    mesosActor.ask(ref => MesosFramework.ExecuteTask(taskType, ref))(5.minutes, scheduler)

  def stop(): Future[Unit] = mesosActor.ask(ref => MesosFramework.Stop(ref))(5.seconds, scheduler)

}

private object MesosFramework {

  val protobufType = MediaType.applicationBinary("x-protobuf", comp = Compressible)

  sealed trait Command
  private final case class Start(replyTo: ActorRef[Unit]) extends Command
  private final case class Started(response: HttpResponse) extends Command
  private final case class ExecuteTask(taskType: TaskType, replyTo: ActorRef[Unit]) extends Command
  private final case class Stop(replyTo: ActorRef[Unit]) extends Command

  private case class Settings(http: HttpExt, apiUrl: String)

  def apply(http: HttpExt, mesosMasterAddress: String)(implicit ec: ExecutionContext) =
    idle(Settings(http, s"http://$mesosMasterAddress/api/v1/scheduler"))

  private def idle(settings: Settings)(
    implicit ec: ExecutionContext
  ): Behavior[Command] =
    Behaviors.receive { (context, message) =>
      message match {
        case Start(replyTo) =>
          subscribe(settings.http, settings.apiUrl).onComplete {
            case Success(response) => context.self ! Started(response)
            case _                 => ???
          }
          starting(settings, replyTo)
        case _ =>
          Behaviors.unhandled
      }
    }

  private def starting(
    settings: Settings,
    startReplyTo: ActorRef[Unit]
  )(
    implicit ec: ExecutionContext
  ): Behavior[Command] = Behaviors.receive { (context, message) =>
    message match {
      case Started(response) =>
        val mesosWorker = context.spawn(MesosWorker(settings.apiUrl), "worker")
        implicit val as = context.system
        val mesosStreamId = response.headers.collectFirst {
          case header if header.name == "Mesos-Stream-Id" => header.value
        }.get
        val streamKillSwitch = response.entity.dataBytes
          .via(RecordIOFraming.scanner())
          .viaMat(KillSwitches.single)(Keep.right)
          .to(
            Sink.foreach { byteString =>
              handleEvent(
                Event.parseFrom(byteString.toArray[Byte]),
                mesosStreamId,
                mesosWorker
              )(context.log)
            }
          )
          .run()
        startReplyTo ! ()
        running(settings, mesosWorker, streamKillSwitch)
      case Stop(stopReplyTo) =>
        startReplyTo ! ()
        stopReplyTo ! ()
        idle(settings)
      case _ => Behaviors.unhandled
    }
  }

  private def running(
    settings: Settings,
    mesosWorker: ActorRef[MesosWorker.InternalEvent],
    streamKillSwitch: KillSwitch
  )(
    implicit ec: ExecutionContext
  ): Behavior[Command] = Behaviors.receive { (context, message) =>
    message match {
      case Stop(replyTo) =>
        context.stop(mesosWorker)
        streamKillSwitch.shutdown()
        replyTo ! ()
        idle(settings)
      case ExecuteTask(taskType, replyTo) =>
        mesosWorker ! MesosWorker.ScheduleTask(MesosWorker.Task(taskType, replyTo))
        Behaviors.same
      case _ =>
        Behaviors.unhandled
    }
  }

  private def subscribe(http: HttpExt, apiUrl: String): Future[HttpResponse] =
    http.singleRequest(
      HttpRequest(
        method = HttpMethods.POST,
        uri = apiUrl,
        headers = List(headers.Accept(protobufType)),
        entity = HttpEntity(
          contentType = ContentType(
            MediaType.applicationBinary("x-protobuf", comp = Compressible)
          ),
          bytes = Call
            .newBuilder()
            .setType(Call.Type.SUBSCRIBE)
            .setSubscribe(
              Subscribe
                .newBuilder()
                .setFrameworkInfo(
                  FrameworkInfo
                    .newBuilder()
                    .setName("Mesos hw framework")
                    .setUser("maxim")
                )
            )
            .build()
            .toByteArray()
        )
      )
    )

  private def handleEvent(mesosEvent: Event, mesosStreamId: String, mesosWorker: ActorRef[MesosWorker.InternalEvent])(
    implicit log: Logger
  ) = {
    val preview = s"Received mesos event: '${mesosEvent.getType}'"
    val message = mesosEvent.getType match {
      case Event.Type.SUBSCRIBED =>
        mesosWorker ! MesosWorker.Subscribed(mesosStreamId, mesosEvent.getSubscribed.getFrameworkId)
        ""
      case Event.Type.OFFERS =>
        mesosWorker ! MesosWorker.Offers(mesosEvent.getOffers.getOffersList.asScala.toList)
        ""
      case Event.Type.UPDATE =>
        mesosWorker ! MesosWorker.Update(mesosEvent.getUpdate())
        s"Update: '${mesosEvent.getUpdate}'"
      case _ =>
        "Skipping event"
    }
    log.info(preview + ". " + message)
  }

}
