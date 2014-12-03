package com.reactmq

import akka.actor.{ ActorLogging, ActorRef }
import akka.stream.actor.{ MaxInFlightRequestStrategy, ActorSubscriber }
import com.reactmq.queue.{ SendMessage, SentMessage }
import akka.stream.actor.ActorSubscriberMessage.{ OnError, OnComplete, OnNext }

class SendToQueueSubscriber(queueActor: ActorRef) extends ActorSubscriber with ActorLogging {

  private var inFlight = 0

  //WatermarkRequestStrategy(2, 1)

  override protected def requestStrategy = new MaxInFlightRequestStrategy(10) {
    override def inFlightInternally = inFlight
  }

  override def receive = {
    case OnNext(msg: String) ⇒
      queueActor ! SendMessage(msg)
      inFlight += 1

    case SentMessage(id) ⇒
      inFlight -= 1

    case OnComplete ⇒
      log.info("OnComplete")

    case OnError(ex) ⇒
      log.info("OnError {}", ex.getMessage)
  }
}
