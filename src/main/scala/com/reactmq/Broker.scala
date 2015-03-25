package com.reactmq

import akka.stream.scaladsl._
import akka.util.ByteString
import java.net.InetSocketAddress
import akka.actor.{ ActorSystem, Props }
import akka.stream.actor.{ ActorPublisher, ActorSubscriber }
import com.reactmq.queue.{ DurableQueue, MessageData, DeleteMessage }

import scala.concurrent.{ Future, Promise }

class Broker(publishersAddress: InetSocketAddress, subscribersAddress: InetSocketAddress)(implicit val system: ActorSystem)
    extends ReactiveStreamsSupport {

  override def run(): Future[Unit] = {
    val compile = Promise[Unit]
    val queue = system.actorOf(DurableQueue.props, name = "queue")
    system.log.info("Binding: [Publishers] {} - [Subscribers] {}", publishersAddress, subscribersAddress)

    val pubs = StreamTcp().bind(publishersAddress)
    val subs = StreamTcp().bind(subscribersAddress)

    val bindPublisherFuture = pubs runForeach { conn ⇒
      system.log.info("New Publishers from: {}", conn.remoteAddress)
      val reconcileFrames = new ReconcileFrames()
      val socketSubscriberQueuePublisher = ActorSubscriber[String](system.actorOf(
        Props(new QueuePublisher(queue, conn.remoteAddress))))

      val deserializeFlow = Flow[ByteString].mapConcat(reconcileFrames.apply)

      conn.flow.via(deserializeFlow)
        .to(Sink(socketSubscriberQueuePublisher))
        .runWith(Source.subscriber)
    }

    val bindSubsFuture = subs runForeach { con ⇒
      system.log.info(s"New Subscriber from: {} ${con.remoteAddress}")
      val reconcileFrames = new ReconcileFrames()

      val socketPublisherQueueSubscriber = ActorPublisher[MessageData](system.actorOf(
        Props(new QueueSubscriber(queue, con.remoteAddress))))

      val source = Source[MessageData](socketPublisherQueueSubscriber)
        .map(m ⇒ Framing.createFrame(m.encodeAsString))

      val confirmSink = Flow[ByteString].mapConcat(reconcileFrames.apply).map { m ⇒
        system.log.info("Confirm delivery for {}", m)
        queue ! DeleteMessage(m)
      }.to(Sink.ignore)

      con.flow.runWith(source, confirmSink)
    }

    handleIOFailure(bindPublisherFuture, "Broker: failed to bind publisher endpoint", Some(compile))
    handleIOFailure(bindSubsFuture, "Broker: failed to bind subs endpoint", Some(compile))
    compile.future
  }
}