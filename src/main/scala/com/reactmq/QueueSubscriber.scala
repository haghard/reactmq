package com.reactmq

import java.net.InetSocketAddress

import akka.actor.{ ActorLogging, ActorRef }
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{ SubscriptionTimeoutExceeded, Cancel, Request }
import com.reactmq.queue.{ ReceivedMessages, ReceiveMessages, MessageData }

class QueueSubscriber(queueActor: ActorRef, remoteAddress: InetSocketAddress) extends ActorPublisher[MessageData]
    with ActorLogging {

  override def postStop() = {
    log.info("Stop {}", self)
  }

  override def receive = {
    case Request(elements) ⇒ if (isActive && totalDemand > 0) {
      queueActor ! ReceiveMessages(elements.toInt)
    }

    case Cancel ⇒
      log.info("Cancellation from {}", remoteAddress)

    case SubscriptionTimeoutExceeded ⇒
      log.info("SubscriptionTimeout {}", remoteAddress)

    //TODO: propagate to queue actor
    case ReceivedMessages(msgs) ⇒ if (isActive) {
      msgs.foreach(onNext)
    } else {
      // TODO: return messages
    }
  }
}
