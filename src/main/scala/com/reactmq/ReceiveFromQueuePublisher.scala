package com.reactmq

import akka.actor.{ ActorLogging, ActorRef }
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }
import com.reactmq.queue.{ ReceivedMessages, ReceiveMessages, MessageData }

class ReceiveFromQueuePublisher(queueActor: ActorRef) extends ActorPublisher[MessageData] {

  override def receive = {
    case Request(elements) ⇒ if (isActive && totalDemand > 0) {
      queueActor ! ReceiveMessages(elements.toInt)
    }
    case Cancel ⇒ // TODO: propagate to queue actor
    case ReceivedMessages(msgs) ⇒ if (isActive) {
      msgs.foreach(onNext)
    } else {
      // TODO: return messages
    }
  }
}
