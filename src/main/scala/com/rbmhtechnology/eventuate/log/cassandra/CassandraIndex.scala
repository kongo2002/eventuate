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

package com.rbmhtechnology.eventuate.log.cassandra

import java.lang.{Long => JLong}

import akka.actor._

import com.datastax.driver.core.{Row, PreparedStatement}
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.EventsourcingProtocol._
import com.rbmhtechnology.eventuate.log.TimeTracker

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent._
import scala.util._

private[eventuate] class CassandraIndex(cassandra: Cassandra, eventReader: CassandraEventReader, logId: String) extends Actor with Stash with ActorLogging {
  import CassandraIndex._
  import CassandraEventLog._
  import context.dispatcher

  private val scheduler = context.system.scheduler
  private val eventLog = context.parent

  private val indexStore = createIndexStore(cassandra, logId)
  private val indexUpdater = context.actorOf(Props(new CassandraIndexUpdater(cassandra, eventReader, indexStore)))

  /**
   * Contains the sequence number of the last event in event log that
   * has been successfully processed and written to the index.
   */
  private var lastIndexTime: TimeTracker = TimeTracker()

  def initializing: Receive = {
    case ReadTimeTracker =>
      indexStore.readTimeTrackerAsync onComplete {
        case Success(t) => self ! ReadTimeTrackerSuccess(t)
        case Failure(e) => self ! ReadTimeTrackerFailure(e)
      }
    case ReadTimeTrackerSuccess(t) =>
      indexUpdater ! UpdateIndex(t)
    case u @ UpdateIndexSuccess(t, _) =>
      this.lastIndexTime = t
      eventLog ! Initialize(t)
      context.become(initialized)
      unstashAll()
      onIndexEvent(u)
    case u @ UpdateIndexFailure(cause) =>
      log.error(cause, "UpdateIndex failure. Retry ...")
      scheduleReadTimeTracker()
      onIndexEvent(u)
    case r @ ReadTimeTrackerFailure(cause) =>
      log.error(cause, "ReadTimeTracker failed. Retry ...")
      scheduleReadTimeTracker()
      onIndexEvent(r)
    case other =>
      stash()
  }

  def initialized: Receive = {
    case UpdateIndex(_) =>
      indexUpdater ! UpdateIndex(lastIndexTime)
    case u @ UpdateIndexSuccess(t, _) =>
      this.lastIndexTime = t
      onIndexEvent(u)
    case u @ UpdateIndexFailure(cause) =>
      log.error(cause, "UpdateIndex failure")
      onIndexEvent(u)
    case Replay(fromSequenceNr, requestor, Some(emitterAggregateId), iid) =>
      val isnr = lastIndexTime.sequenceNr
      val replay = for {
        rsnr <- indexStore.replayAsync(emitterAggregateId, fromSequenceNr)(event => requestor ! Replaying(event, iid))
        nsnr = math.max(isnr, rsnr) + 1L
        s    <- eventReader.replayAsync(nsnr)(event => if (event.destinationAggregateIds.contains(emitterAggregateId)) requestor ! Replaying(event, iid))
      } yield s

      replay onComplete {
        case Success(_) => requestor ! ReplaySuccess(iid)
        case Failure(e) => requestor ! ReplayFailure(e, iid)
      }
  }

  def receive =
    initializing

  private[eventuate] def createIndexStore(cassandra: Cassandra, logId: String) =
    new CassandraIndexStore(cassandra, logId)

  private def scheduleReadTimeTracker(): Unit =
    scheduler.scheduleOnce(cassandra.settings.initRetryBackoff, self, ReadTimeTracker)

  private def updateIncrementAsync(increment: IndexIncrement): Future[IndexIncrement] =
    Future(updateIncrement(increment))(cassandra.readDispatcher)
  
  private def updateIncrement(increment: IndexIncrement): IndexIncrement = {
    eventReader.eventIterator(increment.timeTracker.sequenceNr + 1L, Long.MaxValue).foldLeft(increment) {
      case (inc, event) => inc.update(event)
    }
  }

  override def preStart(): Unit =
    self ! ReadTimeTracker

  // ------------------------------------------------------------------
  //  Test support
  // ------------------------------------------------------------------

  def onIndexEvent(event: Any): Unit = ()
}

private[eventuate] object CassandraIndex {
  case object ReadTimeTracker
  case class ReadTimeTrackerSuccess(timeTracker: TimeTracker)
  case class ReadTimeTrackerFailure(cause: Throwable)

  case class UpdateIndex(timeTracker: TimeTracker = TimeTracker())
  case class UpdateIndexProgress(increment: IndexIncrement)
  case class UpdateIndexSuccess(timeTracker: TimeTracker, steps: Int = 0)
  case class UpdateIndexFailure(cause: Throwable)

  case class AggregateEvents(events: Map[String, Vector[DurableEvent]] = Map.empty) {
    def update(event: DurableEvent): AggregateEvents =
      if (event.destinationAggregateIds.isEmpty) this else copy(event.destinationAggregateIds.foldLeft(events) {
        case (acc, dst) => acc.get(dst) match {
          case Some(events) => acc + (dst -> (events :+ event))
          case None         => acc + (dst -> Vector(event))
        }
      })
  }

  case class IndexIncrement(aggregateEvents: AggregateEvents, timeTracker: TimeTracker) {
    def update(events: Seq[DurableEvent]): IndexIncrement =
      events.foldLeft(this) { case (acc, event) => acc.update(event) }

    def update(event: DurableEvent): IndexIncrement =
      copy(aggregateEvents.update(event), timeTracker.update(event))

    def clearAggregateEvents: IndexIncrement = {
      copy(AggregateEvents(), timeTracker)
    }
  }

  def props(cassandra: Cassandra, eventReader: CassandraEventReader, logId: String): Props =
    Props(new CassandraIndex(cassandra, eventReader, logId: String))
}

private[eventuate] class CassandraIndexUpdater(cassandra: Cassandra, eventReader: CassandraEventReader, indexStore: CassandraIndexStore) extends Actor {
  import CassandraIndex._
  import context.dispatcher

  val index = context.parent

  val idle: Receive = {
    case UpdateIndex(lastIndexTime) =>
      update(lastIndexTime.sequenceNr + 1L, IndexIncrement(AggregateEvents(), lastIndexTime))
      context.become(updating(0))
  }

  def updating(steps: Int): Receive = {
    case UpdateIndexFailure(err) =>
      index ! UpdateIndexFailure(err)
      context.become(idle)
    case UpdateIndexSuccess(t, _) =>
      index ! UpdateIndexSuccess(t, steps)
      context.become(idle)
    case UpdateIndexProgress(inc) =>
      update(inc.timeTracker.sequenceNr + 1L, inc.clearAggregateEvents)
      context.become(updating(steps + 1))
  }

  def receive = idle

  def update(fromSequenceNr: Long, increment: IndexIncrement): Unit =
    updateAsync(fromSequenceNr, increment) onComplete {
      case Success((inc, true))  => self ! UpdateIndexProgress(inc)
      case Success((inc, false)) => self ! UpdateIndexSuccess(inc.timeTracker)
      case Failure(err)          => self ! UpdateIndexFailure(err)
    }
  
  def updateAsync(fromSequenceNr: Long, increment: IndexIncrement): Future[(IndexIncrement, Boolean)] =
    for {
      res <- eventReader.readAsync(fromSequenceNr, cassandra.settings.indexUpdateLimit)
      inc <- writeAsync(increment.update(res.events))
    } yield (inc, res.events.nonEmpty)

  def writeAsync(increment: IndexIncrement): Future[IndexIncrement] =
    indexStore.writeAsync(increment.aggregateEvents, increment.timeTracker).map(_ => increment)
}

private[eventuate] class CassandraIndexStore(cassandra: Cassandra, logId: String) {
  import CassandraIndex._

  private val preparedReadAggregateEventStatement: PreparedStatement = cassandra.prepareReadAggregateEvents(logId)
  private val preparedWriteAggregateEventStatement: PreparedStatement = cassandra.prepareWriteAggregateEvent(logId)

  def replayAsync(aggregateId: String, fromSequenceNr: Long)(f: DurableEvent => Unit): Future[Long] = {
    import cassandra.readDispatcher
    Future(replay(aggregateId, fromSequenceNr)(f))
  }

  def replay(aggregateId: String, fromSequenceNr: Long)(f: DurableEvent => Unit): Long =
    aggregateEventIterator(aggregateId, fromSequenceNr, Long.MaxValue).foldLeft(fromSequenceNr - 1L) {
      case (_, event) => f(event); event.localSequenceNr
    }

  private[eventuate] def readTimeTrackerAsync: Future[TimeTracker] = {
    import cassandra.readDispatcher
    cassandra.session.executeAsync(cassandra.preparedReadTimeTrackerStatement.bind(logId)).map { resultSet =>
      if (resultSet.isExhausted) TimeTracker() else cassandra.timeTrackerFromByteBuffer(resultSet.one().getBytes("time_tracker"))
    }
  }

  private[eventuate] def writeAsync(aggregateEvents: AggregateEvents, timeTracker: TimeTracker)(implicit executor: ExecutionContext): Future[TimeTracker] = {
    for {
      _ <- writeAggregateEventsAsync(aggregateEvents)
      t <- writeTimeTrackerAsync(timeTracker) // must be after other writes
    } yield t
  }

  private def writeAggregateEventsAsync(aggregateEvents: AggregateEvents)(implicit executor: ExecutionContext): Future[Unit] =
    Future.sequence(aggregateEvents.events.map {
      case (aggregateId, events) => writeAggregateEventsAsync(aggregateId, events)
    }).map(_ => ())

  private def writeAggregateEventsAsync(aggregateId: String, events: Seq[DurableEvent])(implicit executor: ExecutionContext): Future[Unit] = cassandra.executeBatchAsync { batch =>
    events.foreach(event => batch.add(preparedWriteAggregateEventStatement.bind(aggregateId, event.localSequenceNr: JLong, cassandra.eventToByteBuffer(event))))
  }

  private def writeTimeTrackerAsync(timeTracker: TimeTracker)(implicit executor: ExecutionContext): Future[TimeTracker] =
    cassandra.session.executeAsync(cassandra.preparedWriteTimeTrackerStatement.bind(logId, cassandra.timeTrackerToByteBuffer(timeTracker))).map(_ => timeTracker)

  private def aggregateEventIterator(aggregateId: String, fromSequenceNr: Long, toSequenceNr: Long): Iterator[DurableEvent] =
    new AggregateEventIterator(aggregateId, fromSequenceNr, toSequenceNr)

  private class AggregateEventIterator(aggregateId: String, fromSequenceNr: Long, toSequenceNr: Long) extends Iterator[DurableEvent] {
    var currentSequenceNr = fromSequenceNr
    var currentIter = newIter()
    var rowCount = 0

    def newIter(): Iterator[Row] =
      if (currentSequenceNr > toSequenceNr) Iterator.empty else cassandra.session.execute(preparedReadAggregateEventStatement.bind(aggregateId, currentSequenceNr: JLong)).iterator.asScala

    @annotation.tailrec
    final def hasNext: Boolean = {
      if (currentIter.hasNext) {
        true
      } else if (rowCount < cassandra.settings.partitionSizeMax) {
        // all events consumed
        false
      } else {
        // max result set size reached, fetch again
        currentSequenceNr += 1L
        currentIter = newIter()
        rowCount = 0
        hasNext
      }
    }

    def next(): DurableEvent = {
      val row = currentIter.next()
      currentSequenceNr = row.getLong("sequence_nr")
      rowCount += 1
      cassandra.eventFromByteBuffer(row.getBytes("event"))
    }
  }
}