package com.reactmq.queue

import akka.actor.{ ActorLogging, Props }
import akka.persistence._
import com.reactmq.util.NowProvider

object DurableDurableDurableDurableDurableQueue {

  def props = Props(new DurableDurableDurableDurableDurableQueue)
}

class DurableDurableDurableDurableDurableQueue extends PersistentActor with ActorLogging
    with DurableQueueStorage
    with DurableDurableQueueReceive
    with DurableQueueRecover {

  override def nowProvider = new NowProvider()

  override def persistenceId = s"${self.path.parent.name}-${self.path.name}"

  override def receiveCommand = handleQueueMsg

  override def receiveRecover = handleQueueEvent

}
