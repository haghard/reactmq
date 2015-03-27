package com.reactmq

import java.net.InetSocketAddress

import akka.actor.{ Props, ActorSystem }
import akka.stream.actor.{ ActorPublisher, ActorSubscriber }
import akka.stream.{ ActorFlowMaterializerSettings, ActorFlowMaterializer }
import akka.stream.scaladsl._
import akka.util.ByteString
import com.reactmq.queue.MessageData
import scala.concurrent.{ Future, Promise }

class Subscriber0(receiveServerAddress: InetSocketAddress)(implicit val system: ActorSystem) extends ReactiveStreamsSupport {

  override def run(): Future[Unit] = {

    system.log.info("Connect to broker: {}", receiveServerAddress)
    val completion = Promise[Unit]()
    val connection = StreamTcp().outgoingConnection(receiveServerAddress)

    val ps = system.actorOf(Props(new DestinationProcessor(completion)))
    val s = ActorSubscriber[MessageData](ps)
    val p = ActorPublisher[ByteString](ps)

    val reconcileFrames = new ReconcileFrames()

    val sink = Flow[ByteString].mapConcat(reconcileFrames.apply).map(MessageData.decodeFromString).to(Sink(s))
    val source = Source[ByteString](p)

    val settings = ActorFlowMaterializerSettings(system)
      .withInputBuffer(initialSize = 2, maxSize = 8)
      .withDispatcher("akka.subscriber-dispatcher")

    connection.to(sink)
      .runWith(source)(ActorFlowMaterializer(settings))

    completion.future
  }
}

/*
  val f = Flow() { implicit builder: FlowGraph.Builder ⇒
    import FlowGraph.Implicits._
    val in = Source.empty[ByteString]
    val out = Sink.publisher[ByteString]
    val mainFlow = Flow[ByteString].mapConcat(reconcileFrames.apply)
      .map(MessageData.decodeFromString)
      .map { md ⇒
        system.log.info(s"Receiver: received msg: $md")
        createFrame(md.id)
      }

    val bcast = builder.add(Broadcast[ByteString](2))
    in ~> bcast ~> mainFlow ~> out
          bcast ~> Sink.onComplete(t ⇒ completion.complete(t))
    (bcast.in, bcast.outlet)
  }

  val g = FlowGraph.closed() { implicit b: FlowGraph.Builder =>
    import FlowGraph.Implicits._
    val in =  Source.subscriber[ByteString]()
    val out = Sink.publisher[ByteString]()

    val mainFlow = Flow[ByteString]
      .mapConcat(reconcileFrames.apply)
      .map(MessageData.decodeFromString)
      .map { md ⇒
      system.log.info(s"Receiver: received msg: $md")
      createFrame(md.id)
    }

    val bcast = b.add(Broadcast[ByteString](2))
    in ~> bcast ~> mainFlow ~> out
          bcast ~> Sink.onComplete(t ⇒ completion.complete(t))
  }
  
*/ 