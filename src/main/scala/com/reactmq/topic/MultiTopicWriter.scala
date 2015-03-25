package com.reactmq.topic

import java.net.InetSocketAddress
import akka.actor.{ ActorLogging, ActorRef, Props }
import com.reactmq.topic.Topics.{ SaveTopicMessage, SentTopicMessage }
import akka.stream.actor.{ ActorSubscriber, MaxInFlightRequestStrategy }
import akka.stream.actor.ActorSubscriberMessage.{ OnComplete, OnError, OnNext }

object MultiTopicWriter {
  def props(topics: ActorRef, address: InetSocketAddress) = Props(new MultiTopicWriter(topics, address))
}

class MultiTopicWriter(topics: ActorRef, address: InetSocketAddress) extends ActorSubscriber with ActorLogging {

  private var inFlight = 0

  override protected def requestStrategy = new MaxInFlightRequestStrategy(10) {
    override def inFlightInternally = inFlight
  }

  override def receive = {
    case OnNext(t: Tweet) ⇒
      inFlight += 1
      topics ! SaveTopicMessage(t)

    case SentTopicMessage(id) ⇒ inFlight -= 1

    case OnComplete ⇒
      log.info("Connection lost with publisher {}", address)
      context.stop(self)

    case OnError(ex) ⇒
      log.info("OnError {}", ex.getMessage)
      context.stop(self)
  }
}