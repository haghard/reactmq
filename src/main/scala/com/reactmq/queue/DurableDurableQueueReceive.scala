package com.reactmq.queue

import scala.collection._
import scala.annotation.tailrec
import akka.actor.{ ActorLogging, ActorRef }
import akka.persistence.{ PersistenceFailure, PersistentActor }

trait DurableDurableQueueReceive extends DurableQueueOps {
  this: DurableQueueStorage with PersistentActor with ActorLogging ⇒

  private val subscribers = new mutable.LinkedHashMap[ActorRef, Int]()

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
    subscribers.headOption match {
      case Some((actor, messageCount)) ⇒
        val received = receiveMessages(messageCount)
        persistAsync(received.map(_._2)) { _ ⇒ }

        if (received != Nil) {
          actor ! ReceivedMessages(received.map(_._1))

          val newMessageCount = messageCount - received.size
          if (newMessageCount > 0) {
            subscribers += (actor -> newMessageCount)
          } else {
            log.info("Remove actor from awaiting {}", actor)
            subscribers -= actor
            tryReply() // maybe we can send more replies
          }
        }
      case _ ⇒
    }
  }

  private def addAwaitingActor(actor: ActorRef, count: Int) {
    subscribers += (actor -> (subscribers.getOrElse(actor, 0) + count))
    log.info("Current subscriber's list was updated {}", subscribers)
  }
}
