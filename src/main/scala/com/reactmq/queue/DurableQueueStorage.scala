package com.reactmq.queue

import scala.collection.mutable
import java.util.UUID
import com.reactmq.util.NowProvider

trait DurableQueueStorage {

  def nowProvider: NowProvider

  //internal state
  protected var messageQueue = mutable.PriorityQueue[InternalMessage]()
  protected val messagesById = mutable.HashMap[String, InternalMessage]()

  case class InternalMessage(
      id: String,
      var nextDelivery: Long,
      content: String) extends Comparable[InternalMessage] {

    // Priority queues have biggest elements first
    def compareTo(other: InternalMessage) = -nextDelivery.compareTo(other.nextDelivery)

    def toMessageData = MessageData(id, content)

    def toMessageAdded = MessageAdded(id, nextDelivery, content)
    def toMessageNextDeliveryUpdated = MessageNextDeliveryUpdated(id, nextDelivery)
  }

  object InternalMessage {
    def from(content: String) = InternalMessage(
      UUID.randomUUID().toString,
      nowProvider.nowMillis,
      content)
  }
}
