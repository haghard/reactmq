package com.reactmq.queue

import akka.actor.Actor.Receive
import akka.actor.ActorLogging
import akka.persistence.{ PersistenceFailure, RecoveryCompleted }

trait DurableQueueRecover {
  this: DurableQueueStorage with ActorLogging ⇒

  def handleQueueEvent: Receive = {
    case MessageAdded(id, nextDelivery, content) ⇒
      messagesById += (id -> InternalMessage(id, nextDelivery, content))
    case MessageNextDeliveryUpdated(id, nextDelivery) ⇒
      messagesById.get(id).foreach(_.nextDelivery = nextDelivery)
    case MessageDeleted(id) ⇒
      messagesById -= id

    case RecoveryCompleted ⇒
      messageQueue ++= messagesById.values
      log.info(s"Undelivered messages size: {}", messagesById.size)

    case PersistenceFailure(payload, seqNum, cause) ⇒
      log.info("Journal fails to write a event: {}", cause.getMessage)
  }
}
