package com.reactmq.topic

import com.reactmq._
import akka.util.ByteString
import akka.stream.scaladsl._
import akka.actor.ActorSystem
import java.net.InetSocketAddress
import com.reactmq.topic.Topics.ConfirmTopicMessage
import akka.stream.actor.{ ActorPublisher, ActorSubscriber }
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }

import scala.concurrent.{ Future, Promise }

class MultiTopicBroker(publishersAddress: InetSocketAddress, subscribersAddress: InetSocketAddress,
                       topicNames: List[String])(implicit val system: ActorSystem) extends ReactiveStreamsSupport {

  val topicTwitterMapping = topicNames.foldLeft(Map[String, String]())((acc, c) ⇒ acc + (c -> c))

  override val materializer = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withInputBuffer(initialSize = 4, maxSize = 16)
      .withDispatcher("akka.topics-dispatcher"))

  override def run(): Future[Unit] = {
    val promise = Promise[Unit]

    val in = Tcp().bind(publishersAddress.getHostString, publishersAddress.getPort)
    val out = Tcp().bind(subscribersAddress.getHostString, subscribersAddress.getPort)
    val topics = system.actorOf(Topics.props(topicNames, topicTwitterMapping), name = "topics")

    system.log.info("Binding: [Publishers] {} - [Subscribers] {}", publishersAddress, subscribersAddress)

    val inFuture = in.runForeach({ con ⇒
      system.log.info("New topics writer from: {}", con.remoteAddress)

      val framer = new Framers()
      val inNetworkSubscriber = ActorSubscriber[Tweet](
        system.actorOf(TopicsWriter.props(topics, con.remoteAddress)))

      val deserializer = Flow[ByteString]
        .mapConcat(framer.apply2)

      con.flow.via(deserializer)
        .to(Sink(inNetworkSubscriber))
        .runWith(Source.subscriber)(materializer)
    })(materializer)

    val outFuture = out.runForeach({ con ⇒
      system.log.info("New topic reader from: {}", con.remoteAddress)

      val sourceN = Source() { implicit b ⇒
        import FlowGraph.Implicits._
        val merge = b.add(Merge[ByteString](topicNames.size))
        topicNames.foreach { name ⇒
          Source(ActorPublisher[ByteString](system.actorOf(TopicReader.props(name, topics)))) ~> merge
        }
        merge.out
      }

      val framer = new Framers()
      val confirmSink = Flow[ByteString].mapConcat(framer.apply).map { m ⇒
        system.log.info("Confirm delivery for {}", m)
        topics ! ConfirmTopicMessage(m)
      }.to(Sink.ignore)

      con.flow.runWith(sourceN, confirmSink)(materializer)
    })(materializer)

    handleIOFailure(inFuture, "In network error", Some(promise))
    handleIOFailure(outFuture, "Out network error", Some(promise))
    promise.future
  }
}