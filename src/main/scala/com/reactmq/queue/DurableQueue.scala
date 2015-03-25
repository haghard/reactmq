package com.reactmq.queue

import akka.persistence._
import com.reactmq.util.NowProvider
import akka.actor.{ ActorLogging, Props }

object DurableQueue {
  def props = Props(new DurableQueue)
}

class DurableQueue extends PersistentActor with ActorLogging
    with DurableQueueStorage
    with DurableQueueRecover
    with DurableDurableQueueReceive {

  override def nowProvider = new NowProvider()

  override def persistenceId = s"${self.path.parent.name}-${self.path.name}"

  override def receiveCommand = handleQueueMsg

  override def receiveRecover = handleQueueEvent

}
