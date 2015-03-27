package com.reactmq

import akka.util.ByteString
import akka.actor.ActorLogging
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
 * @param completion
 */
final class TopicDestinationProcess(completion: Promise[Unit]) extends ActorSubscriber with ActorPublisher[ByteString]
  with ActorLogging {

  private val queue = mutable.Queue[ByteString]()

  override val requestStrategy = new MaxInFlightRequestStrategy(10) {
    override def inFlightInternally() = queue.size
  }

  override def receive: Receive = {
    case OnNext(t: Tweet) ⇒
      log.info("Delivered tweet - {}", t)
      queue += Framing.createFrame(t.id)
      tryReply

    case OnComplete ⇒
      onComplete()
      log.info("OnComplete")
      reconnect()

    case OnError(ex) ⇒
      onError(ex)
      log.info("OnError")
      reconnect()

    case Request(n) ⇒
      tryReply()

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