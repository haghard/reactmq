package com.reactmq

import akka.util.ByteString
import akka.actor.{Props, ActorLogging}
import com.reactmq.topic.Tweet
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }
import akka.stream.actor.ActorSubscriberMessage.{ OnError, OnComplete, OnNext }
import akka.stream.actor.{ MaxInFlightRequestStrategy, ActorPublisher, ActorSubscriber }

import scala.collection.mutable
import scala.concurrent.Promise
import scala.util.Failure

/**
 * Processor implementation inspired by
 * http://bryangilbert.com/blog/2015/02/04/akka-reactive-streams/index.html
 *
 */

object ConfirmationProcessor {
  def prop(completion: Promise[Unit]) =
    Props(classOf[ConfirmationProcessor], completion)
      .withDispatcher("akka.client-dispatcher")
}

class ConfirmationProcessor private(completion: Promise[Unit]) extends ActorSubscriber
    with ActorPublisher[ByteString]
    with ActorLogging {

  //store all unpublished confirmation ids
  val queue = mutable.Queue[ByteString]()

  override val requestStrategy = new MaxInFlightRequestStrategy(10) {
    override val inFlightInternally = queue.size
  }

  override def receive: Receive = {
    case OnNext(t: Tweet) ⇒
      Thread.sleep(500)
      log.info("Delivered tweet: {}", t)
      queue += Framing.createFrame(t.id)
      tryReply

    case OnComplete ⇒
      onComplete()
      log.info("Broker was completed")
      reconnectBroker

    case OnError(ex) ⇒
      onError(ex)
      log.info("OnError {}", ex.getMessage)
      reconnectBroker

    case Request(n) ⇒
      tryReply()

    case Cancel ⇒
      cancel()
      log.info("Cancel")
      reconnectBroker
  }

  def reconnectBroker = {
    context stop self
    completion.complete(Failure(new Exception("Broker connection problem")))
  }

  def tryReply() = {
    while ((isActive && totalDemand > 0) && !queue.isEmpty) {
      onNext(queue.dequeue())
    }
  }
}