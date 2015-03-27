package com.reactmq

import akka.actor.ActorLogging
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }
import akka.stream.actor.ActorSubscriberMessage.{ OnError, OnComplete, OnNext }
import akka.stream.actor.{ MaxInFlightRequestStrategy, ActorPublisher, ActorSubscriber }
import akka.util.ByteString
import com.reactmq.queue.MessageData

import scala.collection.mutable
import scala.concurrent.Promise
import scala.util.Failure

/**
 * If confirmation doesn't delivered he will receive duplicates
 * @param completion
 */
final class DestinationProcessor(completion: Promise[Unit]) extends ActorSubscriber with ActorPublisher[ByteString]
    with ActorLogging {

  private val queue = mutable.Queue[ByteString]()

  override val requestStrategy = new MaxInFlightRequestStrategy(10) {
    override def inFlightInternally() = queue.size
  }

  override def receive = subscriberOps orElse publisherOps

  val subscriberOps: Receive = {
    case OnNext(msg: MessageData) ⇒
      log.info("Delivered message - {}", msg)
      queue.enqueue(Framing.createFrame(msg.id))
      tryReply

    case OnComplete ⇒
      onComplete()
      log.info("OnComplete")
      reconnect()
    case OnError(ex) ⇒
      onError(ex)
      log.info("OnError {}", ex)
      reconnect()
  }

  val publisherOps: Receive = {
    case Request(n) ⇒
      tryReply
    case Cancel ⇒
      cancel()
      log.info("Cancel")
      reconnect()
  }

  private def reconnect() = {
    context.stop(self)
    completion.complete(Failure(new Exception("Broker connection problem")))
  }

  private def tryReply() = {
    if ((isActive && totalDemand > 0) && !queue.isEmpty) {
      val m = queue.dequeue()
      log.info("Confirm - {}", m)
      onNext(m)
    }
  }
}