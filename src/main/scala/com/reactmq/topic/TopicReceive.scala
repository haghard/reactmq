package com.reactmq.topic

import akka.actor.{ ActorLogging, ActorRef }
import akka.persistence.{ PersistenceFailure, PersistentActor }
import com.reactmq.topic.Topics._

import scala.collection.mutable.HashMap

trait TopicReceive extends TopicsOps {
  self: PersistentActor with TopicsStorage with ActorLogging { def twitterTeams: Map[String, String] } ⇒

  type ReceiverRequest = (ActorRef, Int)

  private lazy val receivers = twitterTeams
    .foldLeft(new HashMap[String, HashMap[ActorRef, Int]]()) { (acc, c) ⇒
      acc += (c._1 -> HashMap[ActorRef, Int]())
    }

  protected def handleCommands: Receive = {
    case SendTopicMessage(t) ⇒
      val msg = updateMemory(t)
      persistAsync(msg.toMessageAdded) { msgAdded ⇒
        sender() ! SentTopicMessage(msgAdded.id)
        tryReply()
      }

    case ReceiveTopicMessages(topic, count) ⇒
      log.info("Incoming request on {} for topic {}", count, topic)
      addAwaitingActor(topic, sender(), count)
      tryReply()

    case DeleteTopicMessage(id) ⇒
      deleteMessage(id)
      persistAsync(MessageDeleted(id)) { _ ⇒ }

    case PersistenceFailure(payload, seqNum, cause) ⇒
      log.info("Journal fails to write a event: {}", cause.getMessage)
  }

  private def tryReply() {
    for {
      (topic, subscribers) ← receivers
    } yield {
      subscribers.foreach { kv ⇒
        val newCount = kv._2
        val receiver = kv._1
        val received = receiveMessages(topic, newCount)
        persistAsync(received.map(_._2)) { _ ⇒ }
        if (received != Nil) {
          receiver ! ReceivedTopicMessages(received.map(_._1))

          val newMessageCount = newCount - received.size
          if (newMessageCount > 0) {
            receivers(topic) += receiver -> newMessageCount
            log.info("Update subscribers {}", receivers)
          } else {
            log.info("Remove actor from awaiting {}", receiver)
            receivers(topic) -= receiver
          }
        }
      }
    }
  }

  private def addAwaitingActor(topic: String, actor: ActorRef, count: Int) {
    receivers(topic) += actor -> (receivers(topic).getOrElse(actor, 0) + count)
    log.info("Current receivers for topic {} were updated {}", topic, receivers(topic))
  }
}