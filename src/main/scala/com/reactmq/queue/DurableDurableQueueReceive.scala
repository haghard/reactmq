package com.reactmq.queue

import scala.collection._
import scala.annotation.tailrec
import akka.actor.{ ActorLogging, ActorRef }
import akka.persistence.{ PersistenceFailure, PersistentActor }

trait DurableDurableQueueReceive extends DurableQueueOps {
  this: DurableQueueStorage with PersistentActor with ActorLogging ⇒

  private lazy val receivers = new mutable.LinkedHashMap[ActorRef, Int]()

  def handleQueueMsg: Receive = {
    case SendMessage(content) ⇒
      val msg = sendMessage(content)
      persistAsync(msg.toMessageAdded) { msgAdded ⇒
        sender() ! SentMessage(msgAdded.id)
        tryReply()
      }

    case ReceiveMessages(count) ⇒
      addAwaitingActor(sender(), count)
      tryReply()

    case DeleteMessage(id) ⇒
      deleteMessage(id)
      persistAsync(MessageDeleted(id)) { _ ⇒ }

    case PersistenceFailure(payload, seqNum, cause) ⇒
      log.info("Journal fails to write a event: {}", cause.getMessage)
  }

  @tailrec
  private def tryReply() {
    receivers.headOption match {
      case Some((actor, messageCount)) ⇒
        val received = receiveMessages(messageCount)
        persistAsync(received.map(_._2)) { _ ⇒ }

        if (received != Nil) {
          actor ! ReceivedMessages(received.map(_._1))

          val newMessageCount = messageCount - received.size
          if (newMessageCount > 0) {
            receivers += (actor -> newMessageCount)
            //receivers(actor) = newMessageCount
          } else {
            log.info("Remove actor from awaiting {}", actor)
            receivers -= actor
            tryReply() // maybe we can send more replies
          }
        }
      case _ ⇒
    }
  }

  private def addAwaitingActor(actor: ActorRef, count: Int) {
    receivers += (actor -> (receivers.getOrElse(actor, 0) + count))
    log.info("Current receivers were updated {}", receivers)
  }
}
