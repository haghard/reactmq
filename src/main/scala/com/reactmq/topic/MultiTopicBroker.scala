package com.reactmq.topic

import com.reactmq._
import java.net.InetSocketAddress
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.util.ByteString
import com.reactmq.topic.Topics.ConfirmTopicMessage
import akka.stream.actor.{ ActorPublisher, ActorSubscriber }

import scala.concurrent.{ Future, Promise }

class MultiTopicBroker(publishersAddress: InetSocketAddress, subscribersAddress: InetSocketAddress)(implicit val system: ActorSystem) extends ReactiveStreamsSupport {

  private val topicNames = List("cle", "ind")
  private val topicMapping = Map("cle" -> "cavs", "ind" -> "Pacers")

  override def run(): Future[Unit] = {
    val promise = Promise[Unit]

    val in = Tcp().bind(publishersAddress.getHostString, publishersAddress.getPort)
    val out = Tcp().bind(subscribersAddress.getHostString, subscribersAddress.getPort)
    val topics = system.actorOf(Topics.props(topicNames, topicMapping), name = "topics")

    system.log.info("Binding: [Publishers] {} - [Subscribers] {}", publishersAddress, subscribersAddress)

    val inFuture = in runForeach { con ⇒
      system.log.info("New Publishers from: {}", con.remoteAddress)

      val reconcileFrames = new ReconcileFrames()
      val socketSubscriberTopicsPublisher = ActorSubscriber[Tweet](
        system.actorOf(MultiTopicWriter.props(topics, con.remoteAddress)))

      val deserializeFlow = Flow[ByteString].mapConcat(reconcileFrames.apply2)

      con.flow.via(deserializeFlow)
        .to(Sink(socketSubscriberTopicsPublisher))
        .runWith(Source.subscriber)
    }

    val outFuture = out runForeach { con ⇒
      system.log.info("New publishers from: {}", con.remoteAddress)

      val sourceN = Source() { implicit b ⇒
        import FlowGraph.Implicits._
        val merge = b.add(Merge[ByteString](topicNames.size))
        topicNames.foreach { t ⇒
          Source(ActorPublisher[ByteString](system.actorOf(TopicsReader.props(t, topics)))) ~> merge
        }
        merge.out
      }

      val reconcileFrames = new ReconcileFrames()
      val confirmSink = Flow[ByteString].mapConcat(reconcileFrames.apply).map { m ⇒
        system.log.info("Confirm delivery for {}", m)
        topics ! ConfirmTopicMessage(m)
      }.to(Sink.ignore)

      con.flow.runWith(sourceN, confirmSink)
    }

    handleIOFailure(inFuture, "In network error", Some(promise))
    handleIOFailure(outFuture, "Out network error", Some(promise))
    promise.future
  }
}