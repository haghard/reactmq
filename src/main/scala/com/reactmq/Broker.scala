package com.reactmq

import java.net.InetSocketAddress

import akka.stream.actor.{ ActorPublisher, ActorSubscriber }
import akka.actor.{ ActorSystem, Props }
import akka.io.IO
import akka.stream.io.StreamTcp
import akka.pattern.ask
import akka.stream.scaladsl.{ BlackholeSink, ForeachSink, Source, Sink }
import com.reactmq.queue.{ DurableQueue, MessageData, DeleteMessage }
import Framing._

class Broker(sendServerAddress: InetSocketAddress, receiveServerAddress: InetSocketAddress)(implicit val system: ActorSystem)
    extends ReactiveStreamsSupport {

  def run(): Unit = {
    val ioExt = IO(StreamTcp)
    val bindSendFuture = ioExt ? StreamTcp.Bind(sendServerAddress)
    val bindReceiveFuture = ioExt ? StreamTcp.Bind(receiveServerAddress)

    val queueActor = system.actorOf(DurableQueue.props, name = "queue")

    system.log.info("Binding: Senders address {} Receivers address {}", sendServerAddress, receiveServerAddress)

    bindSendFuture.onSuccess {
      case serverBinding: StreamTcp.TcpServerBinding ⇒
        system.log.info("Broker: send bound")

        Source(serverBinding.connectionStream).runWith(ForeachSink[StreamTcp.IncomingTcpConnection] { conn ⇒
          system.log.info(s"Broker: cluster-sender connected (${conn.remoteAddress})")
          val sendToQueueSubscriber = ActorSubscriber[String](system.actorOf(Props(new SendToQueueSubscriber(queueActor))))

          // sending messages to the queue, receiving from the client
          val reconcileFrames = new ReconcileFrames()
          Source(conn.inputStream)
            .mapConcat(reconcileFrames.apply)
            .runWith(Sink(sendToQueueSubscriber))
        })
    }

    bindReceiveFuture.onSuccess {
      case serverBinding: StreamTcp.TcpServerBinding ⇒
        system.log.info("Broker: receive bound")

        Source(serverBinding.connectionStream).runWith(ForeachSink[StreamTcp.IncomingTcpConnection] { conn ⇒
          system.log.info(s"Broker: cluster-receive client connected (${conn.remoteAddress})")

          val receiveFromQueuePublisher = ActorPublisher[MessageData](system.actorOf(Props(new ReceiveFromQueuePublisher(queueActor))))

          // receiving messages from the queue, sending to the client
          Source(receiveFromQueuePublisher)
            .map(_.encodeAsString)
            .map(createFrame)
            .runWith(Sink(conn.outputStream))

          // replies: ids of messages to delete
          val reconcileFrames = new ReconcileFrames()
          Source(conn.inputStream)
            .mapConcat(reconcileFrames.apply)
            .map(queueActor ! DeleteMessage(_))
            .runWith(BlackholeSink)
        })
    }

    handleIOFailure(bindSendFuture, "Broker: failed to bind send endpoint")
    handleIOFailure(bindReceiveFuture, "Broker: failed to bind receive endpoint")
  }
}

object SimpleBroker extends App with LocalServerSupport {
  new Broker(sendServerAddress, receiveServerAddress).run()
}