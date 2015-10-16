/*
 * Copyright (C) 2015 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rbmhtechnology.eventuate

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.{Function => JFunction}

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout

import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._

import ReplicationProtocol._

class ReplicationSettings(config: Config) {
  val retryInterval: FiniteDuration =
    config.getDuration("eventuate.log.replication.retry-interval", TimeUnit.MILLISECONDS).millis

  val readTimeout: FiniteDuration =
    config.getDuration("eventuate.log.replication.read-timeout", TimeUnit.MILLISECONDS).millis

  val writeTimeout: FiniteDuration =
    config.getDuration("eventuate.log.replication.write-timeout", TimeUnit.MILLISECONDS).millis

  val batchSizeMax: Int =
    config.getInt("eventuate.log.replication.batch-size-max")

  val failureDetectionLimit =
    config.getDuration("eventuate.log.replication.failure-detection-limit", TimeUnit.MILLISECONDS).millis
}

object ReplicationEndpoint {
  /**
   * Default log name.
   */
  val DefaultLogName: String = "default"

  /**
   * Published to the actor system's event stream if a remote log is available.
   */
  case class Available(endpointId: String, logName: String)

  /**
   * Published to the actor system's event stream if a remote log is unavailable.
   */
  case class Unavailable(endpointId: String, logName: String)

  /**
   * Matches a string of format "<hostname>:<port>".
   */
  private object Address {
    def unapply(s: String): Option[(String, Int)] = {
      val hp = s.split(":")
      Some((hp(0), hp(1).toInt))
    }
  }

  /**
   * Creates a [[ReplicationEndpoint]] with a single event log with name [[DefaultLogName]]. The
   * replication endpoint id and replication connections must be configured as follows in `application.conf`:
   *
   * {{{
   *   endpoint.id = "endpoint-id"
   *   endpoint.connections = ["host-1:port-1", "host-2:port-2", ..., "host-n:port-n"]
   * }}}
   *
   * @param logFactory Factory of log actor `Props`. The `String` parameter of the factory is a unique
   *                   log id generated by this endpoint. The log actor must be assigned this log id.
   */
  def apply(logFactory: String => Props)(implicit system: ActorSystem): ReplicationEndpoint = {
    val config = system.settings.config
    val connections = config.getStringList("eventuate.endpoint.connections").asScala.toSet[String].map {
      case Address(host, port) => ReplicationConnection(host, port)
    }
    apply(logFactory, connections)
  }

  /**
   * Creates a [[ReplicationEndpoint]] with a single event log with name [[DefaultLogName]]. The
   * replication endpoint id must be configured as follows in `application.conf`:
   *
   * {{{
   *   endpoint.id = "endpoint-id"
   * }}}
   *
   * @param logFactory Factory of log actor `Props`. The `String` parameter of the factory is a unique
   *                   log id generated by this endpoint. The log actor must be assigned this log id.
   * @param connections Replication connections to other replication endpoints.
   */
  def apply(logFactory: String => Props, connections: Set[ReplicationConnection])(implicit system: ActorSystem): ReplicationEndpoint = {
    val config = system.settings.config
    val endpointId = config.getString("eventuate.endpoint.id")
    new ReplicationEndpoint(endpointId, Set(ReplicationEndpoint.DefaultLogName), logFactory, connections)(system)
  }

  /**
   * Java API.
   *
   * Creates a [[ReplicationEndpoint]] with a single event log with name [[DefaultLogName]]. The
   * replication endpoint id and replication connections must be configured as follows in `application.conf`:
   *
   * {{{
   *   endpoint.id = "endpoint-id"
   *   endpoint.connections = ["host-1:port-1", "host-2:port-2", ..., "host-n:port-n"]
   * }}}
   *
   * @param logFactory Factory of log actor `Props`. The `String` parameter of the factory is a unique
   *                   log id generated by this endpoint. The log actor must be assigned this log id.
   */
  def create(logFactory: JFunction[String, Props], system: ActorSystem) =
    apply(id => logFactory.apply(id))(system)
}

/**
 * A replication endpoint connects to other replication endpoints for replicating events. Events are
 * replicated from the connected endpoints to this endpoint. The connected endpoints are ''replication
 * sources'', this endpoint is a ''replication target''. To setup bi-directional replication, the other
 * replication endpoints must additionally setup replication connections to this endpoint.
 *
 * A replication endpoint manages one or more event logs. Event logs are indexed by name. Events are
 * replicated only between event logs with matching names.
 *
 * @param id Unique replication endpoint id.
 * @param logNames Names of the event logs managed by this replication endpoint.
 * @param logFactory Factory of log actor `Props`. The `String` parameter of the factory is a unique
 *                   log id generated by this endpoint. The log actor must be assigned this log id.
 * @param connections Replication connections to other replication endpoints.
 */
class ReplicationEndpoint(val id: String, val logNames: Set[String], val logFactory: String => Props, val connections: Set[ReplicationConnection])(implicit val system: ActorSystem) {
  import Acceptor._

  private val active: AtomicBoolean =
    new AtomicBoolean(false)

  private[eventuate] val acceptor: ActorRef =
    system.actorOf(Props(new Acceptor(this)), name = Acceptor.Name)

  private[eventuate] val connectors: Set[SourceConnector] =
    connections.map(new SourceConnector(this, _))

  /**
   * The actor system's replication settings.
   */
  val settings =
    new ReplicationSettings(system.settings.config)

  /**
   * This replication endpoint's info object.
   */
  val info: ReplicationEndpointInfo =
    ReplicationEndpointInfo(id, logNames)

  /**
   * The log actors managed by this endpoint, indexed by their name.
   */
  val logs: Map[String, ActorRef] =
    logNames.map(logName => logName -> system.actorOf(logFactory(logId(logName)))).toMap

  /**
   * Returns the unique log id for given `logName`.
   */
  def logId(logName: String): String =
    info.logId(logName)

  /**
   * ...
   */
  def recover(): Future[Unit] = if (!active.getAndSet(true)) {
    import system.dispatcher

    val promise = Promise[Unit]()
    val recovery = new Recovery(this)

    val phase1 = for {
      infos    <- recovery.readEndpointInfos
      trackers <- recovery.readTimeTrackers
      links     = recovery.recoveryLinks(infos, trackers)
      _        <- recovery.deleteSnapshots(links)
    } yield links

    val phase2 = for {
      _ <- phase1
      r <- promise.future
    } yield { active.set(false); r }

    phase1.onSuccess { case links => acceptor ! Recover(links, promise) }
    phase2
  } else Future.failed(new IllegalStateException("Recovery running or endpoint already activated"))

  /**
   * ...
   */
  def activate(): Unit = if (!active.getAndSet(true)) {
    acceptor ! Process
    connectors.foreach(_.activate())
  } else throw new IllegalStateException("Recovery running or endpoint already activated")

  /**
   * Creates [[ReplicationTarget]] for given `logName`.
   */
  private[eventuate] def target(logName: String): ReplicationTarget =
    ReplicationTarget(this, logName, logId(logName), logs(logName))

  /**
   * Returns all log names this endpoint and `endpointInfo` have in common.
   */
  private[eventuate] def commonLogNames(endpointInfo: ReplicationEndpointInfo) =
    this.logNames.intersect(endpointInfo.logNames)
}

/**
 * References a remote event log at a source [[ReplicationEndpoint]].
 */
private case class ReplicationSource(
  endpointId: String,
  logName: String,
  logId: String,
  acceptor: ActorSelection)

/**
 * References a local event log at a target [[ReplicationEndpoint]].
 */
private case class ReplicationTarget(
  endpoint: ReplicationEndpoint,
  logName: String,
  logId: String,
  log: ActorRef) {
}

/**
 * Represents an unidirectional replication link where events are
 * replicated from a `source` to a `target`.
 */
private case class ReplicationLink(
  source: ReplicationSource,
  target: ReplicationTarget)

private class SourceConnector(val targetEndpoint: ReplicationEndpoint, val connection: ReplicationConnection) {
  def links(sourceInfo: ReplicationEndpointInfo): Set[ReplicationLink] =
    targetEndpoint.commonLogNames(sourceInfo).map { logName =>
      val sourceLogId = sourceInfo.logId(logName)
      val source = ReplicationSource(sourceInfo.endpointId, logName, sourceLogId, remoteAcceptor)
      ReplicationLink(source, targetEndpoint.target(logName))
    }

  def activate(): Unit =
    targetEndpoint.system.actorOf(Props(new Connector(this)))

  def remoteAcceptor: ActorSelection =
    remoteActorSelection(Acceptor.Name)

  def remoteActorSelection(actor: String): ActorSelection = {
    import connection._

    val protocol = targetEndpoint.system match {
      case sys: ExtendedActorSystem => sys.provider.getDefaultAddress.protocol
      case sys                      => "akka.tcp"
    }

    targetEndpoint.system.actorSelection(s"${protocol}://${name}@${host}:${port}/user/${actor}")
  }
}

/**
 * Reliably sends [[GetReplicationEndpointInfo]] requests to the [[Acceptor]] at a source [[ReplicationEndpoint]].
 * On receiving a [[GetReplicationEndpointInfoSuccess]] reply, this connector sets up log [[Replicator]]s, one per
 * common log name between source and target endpoints.
 */
private class Connector(sourceConnector: SourceConnector) extends Actor {
  import context.dispatcher

  private val acceptor = sourceConnector.remoteAcceptor
  private var acceptorRequestSchedule: Option[Cancellable] = None

  private var connected = false

  def receive = {
    case GetReplicationEndpointInfoSuccess(info) if !connected =>
      sourceConnector.links(info).foreach {
        case ReplicationLink(source, target) =>
          val filter = sourceConnector.connection.filters.get(target.logName) match {
            case Some(f) => NoFilter.and(f)
            case None    => NoFilter
          }
          context.actorOf(Props(new Replicator(target, source, filter)))

      }
      connected = true
      acceptorRequestSchedule.foreach(_.cancel())
  }

  private def scheduleAcceptorRequest(acceptor: ActorSelection): Cancellable =
    context.system.scheduler.schedule(0.seconds, sourceConnector.targetEndpoint.settings.retryInterval, new Runnable {
      override def run() = acceptor ! GetReplicationEndpointInfo
    })

  override def preStart(): Unit =
    acceptorRequestSchedule = Some(scheduleAcceptorRequest(acceptor))

  override def postStop(): Unit =
    acceptorRequestSchedule.foreach(_.cancel())
}

/**
 * Replicates events from a remote source log to a local target log. This replicator guarantees that
 *
 *  - the ordering of replicated events is preserved
 *  - no duplicates are written to the target log
 *
 *  The second guarantee requires the target log to store the replication progress with read-your-write
 *  consistency to the target log. Whenever writing to the target log fails, the replication progress is
 *  recovered by this replicator and replication is resumed from that state.
 */
private class Replicator(target: ReplicationTarget, source: ReplicationSource, filter: ReplicationFilter) extends Actor with ActorLogging {
  import FailureDetector._
  import target.endpoint.settings
  import context.dispatcher

  val scheduler = context.system.scheduler
  val detector = context.actorOf(Props(new FailureDetector(source.endpointId, source.logName, settings.failureDetectionLimit)))

  var readSchedule: Option[Cancellable] = None

  val fetching: Receive = {
    case GetReplicationProgressSuccess(_, storedReplicationProgress, currentTargetVectorTime) =>
      context.become(reading)
      read(storedReplicationProgress, currentTargetVectorTime)
    case GetReplicationProgressFailure(cause) =>
      log.error(cause, s"replication progress read failed")
      scheduleFetch()
  }

  val idle: Receive = {
    case ReplicationDue =>
      readSchedule.foreach(_.cancel()) // if it's notification from source concurrent to a scheduled read
      context.become(fetching)
      fetch()
  }

  val reading: Receive = {
    case ReplicationReadSuccess(events, replicationProgress, _, currentSourceVectorTime) =>
      detector ! Tick
      context.become(writing)
      write(events, replicationProgress, currentSourceVectorTime)
    case ReplicationReadFailure(cause, _) =>
      log.error(s"replication read failed: $cause")
      context.become(idle)
      scheduleRead()
  }

  val writing: Receive = {
    case ReplicationWriteSuccess(num, _, _) if num == 0 =>
      context.become(idle)
      scheduleRead()
    case ReplicationWriteSuccess(num, storedReplicationProgress, currentTargetVectorTime) =>
      context.become(reading)
      read(storedReplicationProgress, currentTargetVectorTime)
    case ReplicationWriteFailure(cause) =>
      log.error(cause, s"replication write failed")
      context.become(fetching)
      fetch()
  }

  def receive = fetching

  private def scheduleFetch(): Unit =
    scheduler.scheduleOnce(settings.retryInterval)(fetch())

  private def scheduleRead(): Unit =
    readSchedule = Some(scheduler.scheduleOnce(settings.retryInterval, self, ReplicationDue))

  private def fetch(): Unit = {
    implicit val timeout = Timeout(settings.readTimeout)

    target.log ? GetReplicationProgress(source.logId) recover {
      case t => GetReplicationProgressFailure(t)
    } pipeTo self
  }

  private def read(storedReplicationProgress: Long, currentTargetVectorTime: VectorTime): Unit = {
    implicit val timeout = Timeout(settings.readTimeout)

    source.acceptor ? ReplicationReadEnvelope(ReplicationRead(storedReplicationProgress + 1, settings.batchSizeMax, filter, target.logId, self, currentTargetVectorTime), source.logName) recover {
      case t => ReplicationReadFailure(t.getMessage, target.logId)
    } pipeTo self
  }

  private def write(events: Seq[DurableEvent], replicationProgress: Long, currentSourceVectorTime: VectorTime): Unit = {
    implicit val timeout = Timeout(settings.writeTimeout)

    target.log ? ReplicationWrite(events, source.logId, replicationProgress, currentSourceVectorTime) recover {
      case t => ReplicationWriteFailure(t)
    } pipeTo self
  }

  override def preStart(): Unit =
    fetch()

  override def postStop(): Unit =
    readSchedule.foreach(_.cancel())
}

private object FailureDetector {
  case object Tick
}

private class FailureDetector(sourceEndpointId: String, logName: String, failureDetectionLimit: FiniteDuration) extends Actor {
  import FailureDetector._
  import ReplicationEndpoint._

  val failureDetectionLimitMillis = failureDetectionLimit.toMillis
  var lastTick: Long = 0L

  context.setReceiveTimeout(failureDetectionLimit)

  def receive = {
    case Tick =>
      val currentTime = System.currentTimeMillis
      val lastInterval =  currentTime - lastTick
      if (lastInterval >= failureDetectionLimitMillis) {
        context.system.eventStream.publish(Available(sourceEndpointId, logName))
        lastTick = currentTime
      }
    case ReceiveTimeout =>
      context.system.eventStream.publish(Unavailable(sourceEndpointId, logName))
  }
}
