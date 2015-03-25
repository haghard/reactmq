package com.reactmq.queue

import akka.actor.ActorLogging
import scala.annotation.tailrec
import scala.concurrent.duration._
import com.reactmq.util.NowProvider

trait DurableQueueOps {
  this: DurableQueueStorage with ActorLogging ⇒

  private lazy val visibilityTimeout = 10.seconds

  type ReceiveData = (MessageData, MessageNextDeliveryUpdated)

  def nowProvider: NowProvider

  protected def sendMessage(content: String) = {
    val internalMessage = InternalMessage.from(content)
    messageQueue += internalMessage
    messagesById(internalMessage.id) = internalMessage
    log.info("Incoming message: {}", internalMessage)
    internalMessage
  }

  protected def receiveMessages(count: Int): List[ReceiveData] = {
    val deliveryTime = nowProvider.nowMillis

    @tailrec
    def fetchMessages(left: Int, acc: List[ReceiveData]): List[ReceiveData] = {
      if (left == 0) {
        acc
      } else {
        receiveMessage(deliveryTime, computeNextDelivery) match {
          case None      ⇒ acc
          case Some(msg) ⇒ fetchMessages(left - 1, msg :: acc)
        }
      }
    }

    fetchMessages(count, Nil)
  }

  @tailrec
  private def receiveMessage(deliveryTime: Long, newNextDelivery: Long): Option[ReceiveData] = {
    if (messageQueue.size == 0) {
      None
    } else {
      val internalMessage = messageQueue.dequeue()
      val id = internalMessage.id

      if (internalMessage.nextDelivery > deliveryTime) {
        messageQueue += internalMessage
        None
      } else if (messagesById.contains(id)) {
        internalMessage.nextDelivery = newNextDelivery
        messageQueue += internalMessage

        log.info(s"Got message {} for delivery ", id)
        Some(internalMessage.toMessageData, internalMessage.toMessageNextDeliveryUpdated)
      } else {
        receiveMessage(deliveryTime, newNextDelivery)
      }
    }
  }

  private def computeNextDelivery = nowProvider.nowMillis + visibilityTimeout.toMillis

  protected def deleteMessage(id: String) {
    messagesById.remove(id).fold(log.debug(s"Unknown message: $id")) {
      _ ⇒ log.info(s"Deleted confirmed message $id")
    }
  }
}