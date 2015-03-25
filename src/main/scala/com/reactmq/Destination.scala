package com.reactmq

import akka.actor.ActorLogging
import akka.stream.actor.ActorPublisherMessage.{ SubscriptionTimeoutExceeded, Cancel, Request }
import akka.stream.actor.ActorSubscriberMessage.{ OnError, OnComplete, OnNext }
import akka.stream.actor.{ OneByOneRequestStrategy, ActorPublisher, ActorSubscriber }
import akka.util.ByteString
import com.reactmq.queue.MessageData

import scala.collection.mutable
import scala.concurrent.Promise
import scala.util.Failure

/**
 * If confirmation doesn't delivered he will receive duplicates
 * @param completion
 */
final class Destination(completion: Promise[Unit]) extends ActorSubscriber with ActorPublisher[ByteString]
    with ActorLogging {

  private var delayedReqNumber = 0
  private val q = mutable.Queue[ByteString]()

  override protected def requestStrategy = OneByOneRequestStrategy

  override def receive = subscriberOps orElse publisherOps

  private val subscriberOps: Receive = {
    case OnNext(msg: MessageData) ⇒
      log.info("Delivered - {}", msg)
      q.enqueue(Framing.createFrame(msg.id))
      tryToSend

    case OnComplete ⇒
      log.info("OnComplete")
      reconnect()
    case OnError(ex) ⇒
      log.info("OnError")
      reconnect()
  }

  private val publisherOps: Receive = {
    case Request(n) ⇒ delayedReqNumber += n.toInt
    case Cancel ⇒
      log.info("Cancel")
      reconnect()
    case SubscriptionTimeoutExceeded ⇒
      log.info("SubscriptionTimeoutExceeded")
      reconnect()
  }

  private def reconnect() = {
    context.stop(self)
    completion.complete(Failure(new Exception("Broker connection problem")))
  }

  private def tryToSend() = {
    if ((isActive && totalDemand > 0) && (delayedReqNumber > 0)) {
      val s = Math.min(delayedReqNumber, q.size)
      var n = 0
      while (s > n) {
        onNext(q.dequeue())
        n += 1
        delayedReqNumber -= 1
      }
    }
  }
}