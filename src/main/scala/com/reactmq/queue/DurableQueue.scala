package com.reactmq.queue

import akka.actor.{ ActorLogging, Props }
import akka.persistence._
import com.reactmq.util.NowProvider

object DurableQueue {

  def props = Props(new DurableQueue)
}

class DurableQueue extends PersistentActor with ActorLogging
    with DurableQueueStorage
    with DurableDurableQueueReceive
    with DurableQueueRecover {

  override def nowProvider = new NowProvider()

  override def persistenceId = s"${self.path.parent.name}-${self.path.name}"

  override def receiveCommand = handleQueueMsg

  override def receiveRecover = handleQueueEvent

}
