package com.reactmq.topic

import akka.actor.ActorLogging
import akka.persistence.{ PersistenceFailure, PersistentActor, RecoveryCompleted }
import com.reactmq.topic.Topics.{ MessageDeleted, MessageNextDeliveryUpdated, MessageAdded }

trait TopicsRecover {
  self: PersistentActor with TopicsStorage with ActorLogging ⇒

  def handleRecover: Receive = {
    case MessageAdded(id, nextDelivery, tweet) ⇒
      undeliveredId += (id -> InternalMessage(id, nextDelivery, tweet))
    case MessageNextDeliveryUpdated(id, nextDelivery) ⇒
      undeliveredId.get(id).foreach(_.nextDelivery = nextDelivery)
    case MessageDeleted(id) ⇒
      undeliveredId -= id

    case RecoveryCompleted ⇒
      undeliveredId.values.foreach { internalMessage ⇒
        for {
          topic ← internalMessage.t.topic
        } yield { undeliveredTopics(topic) += internalMessage }
      }
      log.info(s"Undelivered messages size: {}", undeliveredId.size)

    case PersistenceFailure(payload, seqNum, cause) ⇒
      log.info("Journal fails to write a event: {}", cause.getMessage)
  }
}