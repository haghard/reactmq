package com.reactmq.topic

import akka.actor.{ ActorRef, Props }
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }
import com.reactmq.topic.Topics.{ ReceivedTopicMessages, ReceiveTopicMessages, MessageData }

object ReceiveFromMultiTopicPublisher {
  def props(topic: String, topics: ActorRef) = Props(new ReceiveFromMultiTopicPublisher(topic, topics))
}

class ReceiveFromMultiTopicPublisher(topic: String, topics: ActorRef) extends ActorPublisher[MessageData] {

  override def receive: Receive = {
    case Request(elements) ⇒ if (isActive && totalDemand > 0) {
      topics ! ReceiveTopicMessages(topic, elements.toInt)
    }
    case ReceivedTopicMessages(msgs) ⇒ if (isActive) {
      msgs.foreach(onNext)
    } else {
      // TODO:
    }
    case Cancel ⇒ // TODO:
  }
}
