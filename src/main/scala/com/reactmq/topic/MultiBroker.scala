package com.reactmq.topic

import java.net.InetSocketAddress
import akka.actor.ActorSystem
import akka.stream.actor.{ ActorPublisher, ActorSubscriber }
import akka.stream.scaladsl._
import akka.util.ByteString
import com.reactmq._
import com.reactmq.topic.Topics.ConfirmTopicMessage

import scala.concurrent.{ Future, Promise }

class MultiBroker(publishersAddress: InetSocketAddress, subscribersAddress: InetSocketAddress)(implicit val system: ActorSystem)
    extends ReactiveStreamsSupport {

  private val tops = List("cle", "ind")
  private val topicsNames = Map("cle" -> "cavs", "ind" -> "Pacers")

  override def run(): Future[Unit] = {
    val promise = Promise[Unit]

    val pubs = StreamTcp().bind(publishersAddress)
    val subs = StreamTcp().bind(subscribersAddress)
    val topics = system.actorOf(Topics.props(tops, topicsNames), name = "topics")

    system.log.info("Binding: [Publishers] {} - [Subscribers] {}", publishersAddress, subscribersAddress)

    val pubsFuture = pubs runForeach { con ⇒
      system.log.info("New Publishers from: {}", con.remoteAddress)

      val reconcileFrames = new ReconcileFrames()
      val socketSubscriberTopicsPublisher = ActorSubscriber[Tweet](
        system.actorOf(MultiTopicWriter.props(topics, con.remoteAddress)))

      val deserializeFlow = Flow[ByteString].mapConcat(reconcileFrames.apply2)

      con.flow.via(deserializeFlow)
        .to(Sink(socketSubscriberTopicsPublisher))
        .runWith(Source.subscriber)
    }

    val subsFuture = subs runForeach { con ⇒
      system.log.info("New publishers from: {}", con.remoteAddress)
      val src = Source(ActorPublisher[ByteString](system.actorOf(TopicsReader.props("cle", topics))))

      val reconcileFrames = new ReconcileFrames()
      val confirmSink = Flow[ByteString].mapConcat(reconcileFrames.apply).map { m ⇒
        system.log.info("Confirm delivery for {}", m)
        topics ! ConfirmTopicMessage(m)
      }.to(Sink.ignore)

      con.flow.runWith(src, confirmSink)
    }

    handleIOFailure(pubsFuture, "Some network error", Some(promise))
    handleIOFailure(subsFuture, "Some network error", Some(promise))
    promise.future
  }
}