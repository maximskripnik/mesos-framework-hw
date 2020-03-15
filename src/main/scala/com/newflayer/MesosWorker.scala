package com.newflayer

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.Http
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.ContentType
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers.RawHeader
import com.google.protobuf.ByteString
import java.util.UUID
import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.Protos.CommandInfo
import org.apache.mesos.v1.Protos.ContainerInfo
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.Protos.Offer.Operation
import org.apache.mesos.v1.Protos.Resource
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskInfo
import org.apache.mesos.v1.Protos.TaskState.TASK_FINISHED
import org.apache.mesos.v1.Protos.TaskState.TASK_RUNNING
import org.apache.mesos.v1.Protos.TaskState.TASK_STAGING
import org.apache.mesos.v1.Protos.TaskState.TASK_STARTING
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Call.Accept
import org.apache.mesos.v1.scheduler.Protos.Call.Acknowledge
import org.apache.mesos.v1.scheduler.Protos.Call.Decline
import org.apache.mesos.v1.scheduler.Protos.Event
import org.slf4j.Logger
import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import com.newflayer.TaskType.BashScript
import com.newflayer.TaskType.SimpleDockerContainer

object MesosWorker {

  case class Task(taskType: TaskType, replyTo: ActorRef[Unit])

  case class Settings(http: HttpExt, apiUrl: String, log: Logger)

  sealed trait InternalEvent

  final case class Subscribed(mesosStreamId: String, frameworkId: FrameworkID) extends InternalEvent
  final case class ScheduleTask(task: Task) extends InternalEvent
  final case class Offers(mesosOffers: List[Offer]) extends InternalEvent
  final case class Update(mesosUpdate: Event.Update) extends InternalEvent

  def apply(apiUrl: String)(
    implicit ec: ExecutionContext
  ): Behavior[InternalEvent] = Behaviors.setup { context =>
    idle(Queue.empty)(ec, Settings(Http()(context.system.toClassic), apiUrl, context.log))
  }

  private def idle(
    queue: Queue[Task]
  )(
    implicit ec: ExecutionContext,
    settings: Settings
  ): Behavior[InternalEvent] =
    Behaviors.receiveMessage {
      case Subscribed(streamId, frameworkId) =>
        running(streamId, frameworkId, queue, Map.empty)
      case ScheduleTask(task) => idle(queue.enqueue(task))
      case _                  => Behaviors.unhandled
    }

  private def running(
    streamId: String,
    frameworkId: FrameworkID,
    queue: Queue[Task],
    runningTasks: Map[TaskID, Task]
  )(
    implicit ec: ExecutionContext,
    settings: Settings
  ): Behavior[InternalEvent] =
    Behaviors.receiveMessage {
      case Offers(offers) =>
        settings.log.info(s"Got offers: '${offers.map(_.getId)}'")
        if (queue.isEmpty) {
          declineOffers(streamId, frameworkId, offers)
          running(streamId, frameworkId, queue, runningTasks)
        } else {
          val acceptableOffer = offers.find((offerIsGoodEnough(_, queue.front.taskType)))
          acceptableOffer match {
            case Some(offer) =>
              val (task, newQueue) = queue.dequeue
              val taskId = TaskID.newBuilder.setValue(UUID.randomUUID().toString).build
              acceptOffer(streamId, frameworkId, offer, task.taskType, taskId)
              declineOffers(streamId, frameworkId, offers.filterNot(_.getId == offer.getId))
              running(streamId, frameworkId, newQueue, runningTasks.updated(taskId, task))
            case None =>
              declineOffers(streamId, frameworkId, offers)
              running(streamId, frameworkId, queue, runningTasks)
          }
        }
      case ScheduleTask(task) =>
        running(streamId, frameworkId, queue.enqueue(task), runningTasks)
      case Update(update) =>
        val taskId = update.getStatus.getTaskId
        val task = runningTasks(taskId)
        val updatedTasks = update.getStatus.getState match {
          case TASK_STAGING | TASK_STARTING | TASK_RUNNING =>
            runningTasks
          case TASK_FINISHED =>
            settings.log.info(s"Task '{}': Finished!", taskId)
            task.replyTo ! ()
            runningTasks.removed(taskId)
          case badStatus =>
            settings.log.error(
              s"Task '$taskId': Bad task status: '$badStatus'! Messsage: '${update.getStatus().getMessage()}'"
            )
            runningTasks.removed(taskId)
        }
        if (update.getStatus.hasUuid) {
          acknowledge(streamId, frameworkId, update.getStatus.getAgentId, taskId, update.getStatus.getUuid)
        }
        running(streamId, frameworkId, queue, updatedTasks)
      case _ =>
        Behaviors.unhandled
    }

  private def declineOffers(
    streamId: String,
    frameworkId: FrameworkID,
    offers: List[Offer]
  )(implicit ec: ExecutionContext, settings: Settings): Future[Unit] = {
    settings.log.info(s"Declining offers '${offers.map(_.getId)}'")
    settings.http
      .singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = settings.apiUrl,
          headers = List(RawHeader("Mesos-Stream-Id", streamId)),
          entity = HttpEntity(
            contentType = ContentType(MesosFramework.protobufType),
            bytes = Call
              .newBuilder()
              .setType(Call.Type.DECLINE)
              .setFrameworkId(frameworkId)
              .setDecline(
                Decline
                  .newBuilder()
                  .addAllOfferIds(
                    offers.map(_.getId).asJava
                  )
              )
              .build()
              .toByteArray()
          )
        )
      )
      .map(_ => ())
  }

  private def offerIsGoodEnough(
    offer: Offer,
    taskType: TaskType
  ): Boolean = {

    def containsDevice(device: Option[String])(resources: List[Resource]): Boolean =
      device match {
        case None =>
          true
        case Some(deviceName) =>
          resources.exists(resource =>
            resource.getName == "devices" && resource.getSet.getItemList.asScala.contains(deviceName)
          )
      }

    def containsResources(cpu: Double, memory: Long, device: Option[String])(resources: List[Resource]): Boolean =
      resources.exists(resource => resource.getName == "cpus" && resource.getScalar.getValue >= cpu) &&
        resources.exists(resource => resource.getName == "mem" && resource.getScalar.getValue >= memory) &&
        containsDevice(device)(resources)

    val predicate: List[Resource] => Boolean = taskType match {
      case SimpleDockerContainer => containsResources(cpu = 0.5, memory = 200L, device = None)
      case _: BashScript         => containsResources(cpu = 1.0, memory = 300L, device = Some("/dev/random"))
    }

    predicate(offer.getResourcesList.asScala.toList)
  }

  private def acceptOffer(
    streamId: String,
    frameworkId: FrameworkID,
    offer: Offer,
    taskType: TaskType,
    taskId: TaskID
  )(implicit ec: ExecutionContext, settings: Settings) = {

    def buildTaskInfo() = taskType match {
      case SimpleDockerContainer =>
        TaskInfo
          .newBuilder()
          .setName("docker hw")
          .setTaskId(taskId)
          .setAgentId(offer.getAgentId)
          .addAllResources(offer.getResourcesList)
          .setCommand(
            CommandInfo
              .newBuilder()
              .setShell(false)
          )
          .setContainer(
            ContainerInfo
              .newBuilder()
              .setType(ContainerInfo.Type.DOCKER)
              .setDocker(
                DockerInfo
                  .newBuilder()
                  .setImage("hello-world")
              )
          )
          .build()
      case BashScript(scriptUrl, arguments) =>
        TaskInfo
          .newBuilder()
          .setName("script hw")
          .setTaskId(taskId)
          .setAgentId(offer.getAgentId)
          .addAllResources(offer.getResourcesList)
          .setCommand(
            CommandInfo
              .newBuilder()
              .addUris(
                CommandInfo.URI
                  .newBuilder()
                  .setValue(scriptUrl)
                  .setExecutable(true)
                  .setCache(false)
                  .setOutputFile("script")
              )
              .setShell(false)
              .setValue("./script")
              .addArguments("this argument is ignored for some reason")
              .addAllArguments(arguments.asJava)
          )
          .build()
    }

    settings.log.info(s"Accepting offer '${offer.getId}'")

    settings.http
      .singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = settings.apiUrl,
          headers = List(RawHeader("Mesos-Stream-Id", streamId)),
          entity = HttpEntity(
            contentType = ContentType(MesosFramework.protobufType),
            bytes = Call
              .newBuilder()
              .setType(Call.Type.ACCEPT)
              .setFrameworkId(frameworkId)
              .setAccept(
                Accept
                  .newBuilder()
                  .addOfferIds(offer.getId)
                  .addOperations(
                    Operation
                      .newBuilder()
                      .setType(Operation.Type.LAUNCH)
                      .setLaunch(
                        Operation.Launch
                          .newBuilder()
                          .addTaskInfos(buildTaskInfo())
                      )
                  )
              )
              .build()
              .toByteArray()
          )
        )
      )
      .map(_ => ())
  }

  private def acknowledge(
    streamId: String,
    frameworkId: FrameworkID,
    agentId: AgentID,
    taskId: TaskID,
    statusUUID: ByteString
  )(implicit ec: ExecutionContext, settings: Settings): Future[Unit] =
    settings.http
      .singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = settings.apiUrl,
          headers = List(RawHeader("Mesos-Stream-Id", streamId)),
          entity = HttpEntity(
            contentType = ContentType(MesosFramework.protobufType),
            bytes = Call
              .newBuilder()
              .setType(Call.Type.ACKNOWLEDGE)
              .setFrameworkId(frameworkId)
              .setAcknowledge(
                Acknowledge
                  .newBuilder()
                  .setAgentId(agentId)
                  .setTaskId(taskId)
                  .setUuid(statusUUID)
              )
              .build()
              .toByteArray()
          )
        )
      )
      .map(_ => ())
}
