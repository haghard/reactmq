package com.reactmq.topic

import akka.actor.{ ActorLogging, ActorRef, Props }
import akka.stream.actor.ActorSubscriberMessage.{ OnComplete, OnError, OnNext }
import akka.stream.actor.{ ActorSubscriber, MaxInFlightRequestStrategy }
import com.reactmq.topic.Topics.{ SendTopicMessage, SentTopicMessage }

object SendToMultiTopicSubscriber {
  def props(topics: ActorRef) = Props(new SendToMultiTopicSubscriber(topics))
}

class SendToMultiTopicSubscriber(topics: ActorRef) extends ActorSubscriber with ActorLogging {

  private var inFlight = 0

  override protected def requestStrategy = new MaxInFlightRequestStrategy(10) {
    override def inFlightInternally = inFlight
  }

  override def receive = {
    case OnNext(t: Tweet) ⇒
      topics ! SendTopicMessage(t)

    case SentTopicMessage(id) ⇒ inFlight -= 1

    case OnComplete ⇒
      log.info("OnComplete")

    case OnError(ex) ⇒
      log.info("OnError {}", ex.getMessage)
      context.stop(self)
  }
}
