package com.reactmq.cluster

import akka.util.ByteString
import com.reactmq.topic.Tweet
import java.net.InetSocketAddress
import scala.concurrent.{ Promise, Future }
import akka.actor.{ Props, ActorSystem }
import akka.stream.actor.{ ActorPublisher, ActorSubscriber }
import akka.stream.scaladsl.{ Source, Sink, Flow, StreamTcp }
import com.reactmq.{ TDestination, ReconcileFrames, ReactiveStreamsSupport }
import akka.stream.{ ActorFlowMaterializer, ActorFlowMaterializerSettings }

object ClusterTopicsSubscriber extends App with ClusterClientSupport {
  start("topic-subscriber", (ba, system) ⇒ new TopicsSubscriber(ba.subscribersAddress)(system).run())
}

final class TopicsSubscriber(receiveServerAddress: InetSocketAddress)(implicit val system: ActorSystem) extends ReactiveStreamsSupport {

  override def run(): Future[Unit] = {
    system.log.info("Connect to broker: {}", receiveServerAddress)

    val completion = Promise[Unit]()
    val connection = StreamTcp().outgoingConnection(receiveServerAddress)

    val ps = system.actorOf(Props(new TDestination(completion)))
    val s = ActorSubscriber[Tweet](ps)
    val p = ActorPublisher[ByteString](ps)

    val reconcileFrames = new ReconcileFrames()

    val sink = Flow[ByteString].mapConcat(reconcileFrames.apply2).to(Sink(s))
    val source = Source[ByteString](p)

    connection.runWith(source, sink)(ActorFlowMaterializer(
      ActorFlowMaterializerSettings(system)
        .withInputBuffer(initialSize = 2, maxSize = 4)
        .withDispatcher("akka.subscriber-dispatcher")))

    completion.future
  }
}