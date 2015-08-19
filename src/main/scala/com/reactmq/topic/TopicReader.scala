package com.reactmq.topic

import com.reactmq.Framing
import akka.util.ByteString
import akka.actor.{ ActorLogging, ActorRef, Props }
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }
import com.reactmq.topic.Topics.{ ReceivedTopicMessages, ReceiveTopicMessages }
import com.reactmq.topic.TopicReader.EraseSubscriber

object TopicReader {
  case class EraseSubscriber(topicName: String, actor: ActorRef)

  def props(name: String, topics: ActorRef) = Props(new TopicReader(name, topics))
}

class TopicReader(name: String, topics: ActorRef) extends ActorPublisher[ByteString] with ActorLogging {

  override def receive: Receive = {
    case Request(elements) ⇒ if (isActive && totalDemand > 0) {
      topics ! ReceiveTopicMessages(name, elements.toInt)
    }
    case ReceivedTopicMessages(msgs) ⇒ if (isActive) {
      msgs.foreach(m ⇒ onNext(Framing.toBytes(m.tweet.copy(id = m.id))))
    } else {
      log.info("***** Not active *************")
    }
    case Cancel ⇒
      topics ! EraseSubscriber(name, self)
      context stop self
      log.info("*****Cancel TopicReader *****{}", self)
  }
}
