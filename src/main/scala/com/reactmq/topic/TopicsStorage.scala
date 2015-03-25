package com.reactmq.topic

import java.util.UUID

import com.reactmq.topic.Topics.{ MessageNextDeliveryUpdated, MessageAdded, MessageData }
import com.reactmq.util.NowProvider

import scala.collection.mutable

trait TopicsStorage {
  self: { def teams: List[String] } ⇒

  def nowProvider: NowProvider

  //internal state
  protected var messageQueues = teams
    .foldLeft(mutable.Map[String, mutable.PriorityQueue[InternalMessage]]()) { (acc, c) ⇒
      acc += c -> mutable.PriorityQueue[InternalMessage]()
    }

  protected val messagesById = mutable.HashMap[String, InternalMessage]()

  case class InternalMessage(id: String, var nextDelivery: Long, t: Tweet) extends Comparable[InternalMessage] {

    // Priority queues have biggest elements first
    def compareTo(other: InternalMessage) = -nextDelivery.compareTo(other.nextDelivery)

    def toMessageData = MessageData(id, t)

    def toMessageAdded = MessageAdded(id, nextDelivery, t)
    def toMessageNextDeliveryUpdated = MessageNextDeliveryUpdated(id, nextDelivery)
  }

  object InternalMessage {
    def from(t: Tweet) = InternalMessage(UUID.randomUUID().toString, nowProvider.nowMillis, t)
  }
}
