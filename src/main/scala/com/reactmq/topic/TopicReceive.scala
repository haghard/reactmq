package com.reactmq.topic

import akka.actor.{ ActorLogging, ActorRef }
import akka.persistence.PersistentActor
import com.reactmq.topic.Topics._
import com.reactmq.topic.TopicReader.EraseSubscriber

import scala.collection.mutable.HashMap

trait TopicReceive extends TopicsOps {
  self: PersistentActor with TopicsStorage with ActorLogging ⇒

  def twitterTeams: Map[String, String]

  type ReceiverRequest = (ActorRef, Int)

  private val topicWaiters = twitterTeams
    .foldLeft(new HashMap[String, HashMap[ActorRef, Int]]()) { (acc, c) ⇒
      acc += (c._1 -> HashMap[ActorRef, Int]())
    }

  protected def handleCommands: Receive = {
    case SaveTopicMessage(t) ⇒
      val msg = updateMemory(t)
      persistAsync(msg.toMessageAdded) { msgAdded ⇒
        sender() ! SentTopicMessage(msgAdded.id)
        tryReply()
      }

    case ReceiveTopicMessages(topic, count) ⇒
      log.info("Incoming request on {} for topic {}", count, topic)
      submitWaiter(topic, sender(), count)
      tryReply()

    case ConfirmTopicMessage(id) ⇒
      deleteMessage(id)
      persistAsync(MessageDeleted(id)) { _ ⇒ }

    case EraseSubscriber(topicName, actor) ⇒
      val clean = topicWaiters(topicName) - actor
      topicWaiters += (topicName -> clean)
      log.info("EraseSubscriber topic {}  {}", topicName, topicWaiters)

    //case PersistenceFailure(payload, seqNum, cause) ⇒ log.info("Journal fails to write a event: {}", cause.getMessage)
  }

  private def tryReply() {
    for {
      (topic, subscribers) ← topicWaiters
    } yield {
      subscribers.foreach { kv ⇒
        val reqSize = kv._2
        val requester = kv._1
        val received = receiveMessages(topic, reqSize)
        persistAsync(received.map(_._2)) { _ ⇒ }
        if (received != Nil) {
          requester ! ReceivedTopicMessages(received.map(_._1))
          val reducedReqSize = reqSize - received.size
          if (reducedReqSize > 0) {
            topicWaiters(topic) += requester -> reducedReqSize
            //log.info("Update topicWaiters {}", topicWaiters)
          } else {
            //log.info("Remove waiter from waiters {}", receiver)
            topicWaiters(topic) -= requester
          }
        }
      }
    }
  }

  private def submitWaiter(topic: String, actor: ActorRef, reqSize: Int) {
    topicWaiters(topic) += actor -> (topicWaiters(topic).getOrElse(actor, 0) + reqSize)
    log.info("Submit waiter for topic {} with size {}", topic, reqSize)
  }
}