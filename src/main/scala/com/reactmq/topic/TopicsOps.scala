package com.reactmq.topic

import akka.actor.ActorLogging
import com.reactmq.util.NowProvider

import scala.annotation.tailrec
import scala.concurrent.duration._

import com.reactmq.topic.Topics.{ MessageData, MessageNextDeliveryUpdated }

trait TopicsOps {
  self: TopicsStorage with ActorLogging ⇒

  private lazy val visibilityTimeout = 20.seconds

  type Received = (MessageData, MessageNextDeliveryUpdated)

  def nowProvider: NowProvider

  protected def updateMemory(t: Tweet) = {
    val internalMessage = InternalMessage.from(t)
    for {
      t ← t.topic
      q ← messageQueues.get(t)
    } yield {
      q += internalMessage
      messagesById(internalMessage.id) = internalMessage
      log.info("Incoming message for topic: {} message: {}", t, internalMessage)
    }

    internalMessage
  }

  protected def receiveMessages(topic: String, count: Int): List[Received] = {

    val deliveryTime = nowProvider.nowMillis

    @tailrec
    def doReceiveMessages(topic: String, left: Int, acc: List[Received]): List[Received] = {
      if (left == 0) {
        acc
      } else {
        receiveMessage(topic, deliveryTime, computeNextDelivery) match {
          case None      ⇒ acc
          case Some(msg) ⇒ doReceiveMessages(topic, left - 1, msg :: acc)
        }
      }
    }

    doReceiveMessages(topic, count, Nil)
  }

  private def receiveMessage(topic: String, deliveryTime: Long, newNextDelivery: Long): Option[Received] =
    messageQueues.get(topic).filter(_.size > 0)
      .flatMap { q ⇒
        val internalMessage = q.dequeue()
        val id = internalMessage.id
        if (internalMessage.nextDelivery > deliveryTime) {
          q += internalMessage
          None
        } else if (messagesById.contains(id)) {
          internalMessage.nextDelivery = newNextDelivery
          q += internalMessage

          log.info(s"Got message for topic {} message: {} ", topic, id)
          Some(internalMessage.toMessageData, internalMessage.toMessageNextDeliveryUpdated)
        } else {
          receiveMessage(topic, deliveryTime, newNextDelivery)
        }
      }

  private def computeNextDelivery = nowProvider.nowMillis + visibilityTimeout.toMillis

  protected def deleteMessage(id: String) {
    messagesById.remove(id).fold(log.debug(s"Unknown message: $id")) {
      _ ⇒ log.info(s"Deleted confirmed message $id")
    }
  }
}