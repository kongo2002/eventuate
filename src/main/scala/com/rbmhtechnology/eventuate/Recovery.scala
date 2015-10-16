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

import akka.actor._
import akka.pattern.{after, ask}
import akka.util.Timeout

import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.ReplicationProtocol._
import com.rbmhtechnology.eventuate.log.TimeTracker
import com.typesafe.config.Config

import scala.collection.immutable.Seq
import scala.concurrent._
import scala.concurrent.duration._

private case class RecoveryLink(logName: String, localLogId: String, remoteLogId: String, tracker: TimeTracker)

private class RecoverySettings(config: Config) {
  val remoteOperationRetryMax: Int =
    config.getInt("eventuate.disaster-recovery.remote-operation-retry-max")

  val remoteOperationRetryDelay: FiniteDuration =
    config.getDuration("eventuate.disaster-recovery.remote-operation-retry-delay", TimeUnit.MILLISECONDS).millis

  val remoteOperationTimeout: FiniteDuration =
    config.getDuration("eventuate.disaster-recovery.remote-operation-timeout", TimeUnit.MILLISECONDS).millis

  val snapshotDeletionTimeout: FiniteDuration =
    config.getDuration("eventuate.disaster-recovery.snapshot-deletion-timeout", TimeUnit.MILLISECONDS).millis
}

private class Recovery(endpoint: ReplicationEndpoint) {
  private val settings = new RecoverySettings(endpoint.system.settings.config)

  import Recovery._
  import settings._
  import endpoint.system.dispatcher

  private implicit val timeout = Timeout(remoteOperationTimeout)
  private implicit val scheduler = endpoint.system.scheduler

  // -------------------------------------------------------------------------
  //  This class assumes that replication between endpoints is bi-directional
  //  (which is actually the case for most if not all Eventuate applications)
  // -------------------------------------------------------------------------

  def readTimeTrackers: Future[Map[String, TimeTracker]] = {
    println(s"[recovery of ${endpoint.id}] Read time trackers from local event logs ...")
    Future.sequence(endpoint.logNames.map(name => readTimeTracker(endpoint.logs(name)).map(name -> _))).map(_.toMap)
  }

  def readEndpointInfos: Future[Set[ReplicationEndpointInfo]] = {
    println(s"[recovery of ${endpoint.id}] Read metadata from remote replication endpoints ...")
    Future.sequence(endpoint.connectors.map(connector => readEndpointInfo(connector.remoteAcceptor)))
  }

  def deleteSnapshots(links: Set[RecoveryLink]): Future[Unit] = {
    println(s"[recovery of ${endpoint.id}] Delete invalidated snapshots at local endpoint ...")
    Future.sequence(links.map(deleteSnapshots)).map(_ => ())
  }

  def readTimeTracker(targetLog: ActorRef): Future[TimeTracker] =
    targetLog.ask(GetTimeTracker).mapTo[GetTimeTrackerSuccess].map(_.tracker)

  def readEndpointInfo(targetAcceptor: ActorSelection): Future[ReplicationEndpointInfo] =
    retry(targetAcceptor.ask(GetReplicationEndpointInfo), remoteOperationRetryDelay, remoteOperationRetryMax).mapTo[GetReplicationEndpointInfoSuccess].map(_.info)

  def deleteSnapshots(link: RecoveryLink): Future[Unit] =
    endpoint.logs(link.logName).ask(DeleteSnapshots(link.tracker.sequenceNr + 1L))(Timeout(snapshotDeletionTimeout)).flatMap {
      case DeleteSnapshotsSuccess    => Future.successful(())
      case DeleteSnapshotsFailure(e) => Future.failed(e)
    }

  def recoveryLinks(endpointInfos: Set[ReplicationEndpointInfo], timeTrackers: Map[String, TimeTracker]) = for {
    endpointInfo <- endpointInfos
    logName      <- endpoint.commonLogNames(endpointInfo)
  } yield RecoveryLink(logName, endpoint.logId(logName), endpointInfo.logId(logName), timeTrackers(logName))
}

private object Recovery {
  def retry[T](async: => Future[T], delay: FiniteDuration, retries: Int)(implicit ec: ExecutionContext, s: Scheduler): Future[T] =
    async recoverWith { case _ if retries > 0 => after(delay, s)(retry(async, delay, retries - 1)(ec, s)) }
}

private class Acceptor(endpoint: ReplicationEndpoint) extends Actor {
  import Acceptor._

  def initializing: Receive = {
    case Process =>
      context.become(processing)
    case Recover(links, promise) =>
      println(s"[recovery of ${endpoint.id}] Chacking replication progress with remote endpoints ...")
      context.become(recovering(context.actorOf(Props(new InconsistencyCompensation(endpoint.id, links))), promise))
  }

  def recovering(recovery: ActorRef, promise: Promise[Unit]): Receive = {
    case re: ReplicationReadEnvelope =>
      recovery forward re
    case RecoveryCompleted =>
      promise.success(())
      context.become(processing)
  }

  def processing: Receive = {
    case ReplicationReadEnvelope(r, logName) =>
    endpoint.logs(logName) forward r
  }

  override def unhandled(message: Any): Unit = message match {
    case GetReplicationEndpointInfo =>
      sender() ! GetReplicationEndpointInfoSuccess(endpoint.info)
    case _ =>
      super.unhandled(message)
  }

  def receive =
    initializing
}

private object Acceptor {
  val Name = "acceptor"

  case object Process
  case class Recover(links: Set[RecoveryLink], promise: Promise[Unit])
  case class RecoveryStepCompleted(link: RecoveryLink)
  case object RecoveryCompleted
}

private class InconsistencyCompensation(endpointId: String, links: Set[RecoveryLink]) extends Actor {
  import Acceptor._

  var active = links

  var compensators: Map[String, ActorRef] =
    links.map(link => link.remoteLogId -> context.actorOf(Props(new InconsistencyCompensator(endpointId, link)))).toMap

  def receive = {
    case ReplicationReadEnvelope(r, _) =>
      compensators(r.targetLogId) forward r
    case RecoveryStepCompleted(link) if active.contains(link) =>
      active = active - link
      val prg = links.size - active.size
      val all = links.size
      println(s"[recovery of ${endpointId}] Confirm existence of consistent replication progress at ${link.remoteLogId} ($prg of $all) ...")
      if (active.isEmpty) context.parent ! RecoveryCompleted
  }
}

/**
 * Processes [[ReplicationRead]] requests and checks if their `fromSequenceNr` exceeds the valid read range of
 * the local log. The `fromSequenceNr` is determined by the replication progress stored at the remote log. In
 * case the valid read range is exceeded, the replication progress of the empty reply is set to the local log's
 * current sequence number so that the remote log updates the stored replication progress accordingly. Subsequent
 * reads will therefore be within the valid read range. On receiving a valid read range, the parent actor is
 * notified so that recovery progress can be tracked.
 */
private class InconsistencyCompensator(endpointId: String, link: RecoveryLink) extends Actor {
  import Acceptor._

  def receive = {
    case r: ReplicationRead if r.fromSequenceNr > link.tracker.sequenceNr + 1L =>
      println(s"[recovery of ${endpointId}] Trigger update of inconsistent replication progress at ${link.remoteLogId} ...")
      sender() ! ReplicationReadSuccess(Seq(), link.tracker.sequenceNr, link.remoteLogId, link.tracker.vectorTime)
    case r: ReplicationRead =>
      sender() ! ReplicationReadSuccess(Seq(), r.fromSequenceNr - 1L, link.remoteLogId, link.tracker.vectorTime)
      context.parent ! RecoveryStepCompleted(link)
  }
}

