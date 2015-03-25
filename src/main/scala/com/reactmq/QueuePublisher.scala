package com.reactmq

import java.net.InetSocketAddress

import akka.actor.{ ActorLogging, ActorRef }
import akka.stream.actor.{ MaxInFlightRequestStrategy, ActorSubscriber }
import com.reactmq.queue.{ SendMessage, SentMessage }
import akka.stream.actor.ActorSubscriberMessage.{ OnError, OnComplete, OnNext }

class QueuePublisher(queue: ActorRef, add: InetSocketAddress) extends ActorSubscriber with ActorLogging {

  private var inFlight = 0

  override protected def requestStrategy = new MaxInFlightRequestStrategy(10) {
    override def inFlightInternally = inFlight
  }

  override def receive = {
    case OnNext(msg: String) ⇒
      queue ! SendMessage(msg)
      inFlight += 1

    case SentMessage(id) ⇒
      inFlight -= 1

    case OnComplete  ⇒ log.info("Publisher {} was disconnected", add)

    case OnError(ex) ⇒ log.info("OnError {}", ex.getMessage)
  }
}
