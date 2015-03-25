package com.reactmq

import akka.actor.ActorLogging
import akka.stream.actor.ActorPublisherMessage.{ SubscriptionTimeoutExceeded, Cancel, Request }
import akka.stream.actor.ActorSubscriberMessage.{ OnError, OnComplete, OnNext }
import akka.stream.actor.{ OneByOneRequestStrategy, ActorPublisher, ActorSubscriber }
import akka.util.ByteString
import com.reactmq.topic.Tweet

import scala.collection.mutable
import scala.concurrent.Promise
import scala.util.Failure

final class TDestination(completion: Promise[Unit]) extends ActorSubscriber with ActorPublisher[ByteString] with ActorLogging {
  private var delayedReqNumber = 0
  private val confirmations = mutable.Queue[ByteString]()

  override protected def requestStrategy = OneByOneRequestStrategy

  override def receive: Receive = {
    case OnNext(t: Tweet) ⇒
      log.info("Delivered tweet - {}", t)
      confirmations += Framing.createFrame(t.id)
      tryToSend()
    case OnComplete ⇒
      log.info("OnComplete")
      reconnect()
    case OnError(ex) ⇒
      log.info("OnError")
      reconnect()

    case Request(n) ⇒
      delayedReqNumber += n.toInt

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
      val s = Math.min(delayedReqNumber, confirmations.size)
      var n = 0
      while (s > n) {
        onNext(confirmations.dequeue())
        n += 1
        delayedReqNumber -= 1
      }
    }
  }
}