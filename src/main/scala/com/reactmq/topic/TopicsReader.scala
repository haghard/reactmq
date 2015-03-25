package com.reactmq.topic

import com.reactmq.Framing
import akka.util.ByteString
import akka.actor.{ ActorRef, Props }
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }
import com.reactmq.topic.Topics.{ ReceivedTopicMessages, ReceiveTopicMessages }

object TopicsReader {
  def props(name: String, topics: ActorRef) = Props(new TopicsReader(name, topics))
}

class TopicsReader(name: String, topics: ActorRef) extends ActorPublisher[ByteString] {

  override def receive: Receive = {
    case Request(elements) ⇒ if (isActive && totalDemand > 0) {
      topics ! ReceiveTopicMessages(name, elements.toInt)
    }
    case ReceivedTopicMessages(msgs) ⇒ if (isActive) {
      msgs.foreach(m ⇒ onNext(Framing.toBytes(m.tweet.copy(id = m.id))))
    } else {
      // TODO:
    }
    case Cancel ⇒ // TODO:
  }
}
