package com.reactmq.topic

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.stream.actor.{ ActorPublisher, ActorSubscriber }
import akka.stream.io.StreamTcp
import akka.stream.scaladsl.{ BlackholeSink, Sink, Source }
import com.reactmq.topic.Topics.MessageData
import com.reactmq.{ Framing, LocalServerSupport, ReactiveStreamsSupport, ReconcileFrames }

import scala.concurrent.Promise
import scala.util.{ Failure, Success }

class BrokerN(sendServerAddress: InetSocketAddress, receiveServerAddress: InetSocketAddress)(implicit val system: ActorSystem)
    extends ReactiveStreamsSupport {

  def run: scala.concurrent.Future[Unit] = {
    val promise = Promise[Unit]
    val ioExt = IO(StreamTcp)
    val bindSendFuture = ioExt ? StreamTcp.Bind(sendServerAddress)
    //val bindReceiveFuture = ioExt ? StreamTcp.Bind(receiveServerAddress)

    val topics = system.actorOf(Topics.props(List("cle", "ind"), Map("cle" -> "cavs", "ind" -> "Pacers")), name = "topics")

    bindSendFuture.onSuccess {
      case serverBinding: StreamTcp.TcpServerBinding ⇒
        system.log.info("Broker wait for senders on {}", serverBinding.localAddress)
        val message = new StringBuilder()
          .append('\n')
          .append("================================================================================================")
          .append('\n')
          .append(s"★ ★ ★ ★ ★ ★  Broker started addresses $sendServerAddress ★ ★ ★ ★ ★ ★")
          .append('\n')
          .append("================================================================================================")
          .append('\n').toString()
        system.log.info(message)

        Source(serverBinding.connectionStream) foreach { conn ⇒
          system.log.info("Broker new sender from {}", conn.remoteAddress)
          val reconcileFrames = new ReconcileFrames()

          Source(conn.inputStream)
            .mapConcat(reconcileFrames.apply2)
            .to(Sink(ActorSubscriber[Tweet](system.actorOf(SendToMultiTopicSubscriber.props(topics)))))
            .run()

          //simulate incoming receivers
          Source(ActorPublisher[MessageData](system.actorOf(ReceiveFromMultiTopicPublisher.props("cle", topics))))
            .map { m ⇒ { system.log.info("Receive from [cle] : {}", m.encodeAsString); Framing.toBytes(m.tweet) } }
            .runWith(BlackholeSink)

          Source(ActorPublisher[MessageData](system.actorOf(ReceiveFromMultiTopicPublisher.props("ind", topics))))
            .map { m ⇒ { system.log.info("Receive from [ind] : {}", m.encodeAsString); Framing.toBytes(m.tweet) } }
            .runWith(BlackholeSink)
        }
    }

    handleIOFailure(bindSendFuture, "binding error", Some(promise))
    promise.future
  }
}

object MultiTopicsBroker extends App with LocalServerSupport {
  import system.dispatcher
  new BrokerN(sendServerAddress, receiveServerAddress).run
    .onComplete {
      case Success(r) ⇒
        system.log.info("Broker was started")
      case Failure(ex) ⇒
        system.log.info("{}", ex.getMessage)
        system.shutdown()
    }
}
