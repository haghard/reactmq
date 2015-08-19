package com.reactmq

import java.net.InetSocketAddress

import akka.actor.{ Props, ActorSystem }
import akka.stream.actor.{ ActorPublisher, ActorSubscriber }
import akka.stream.{ ActorMaterializerSettings, ActorMaterializer }
import akka.stream.scaladsl._
import akka.util.ByteString
import com.reactmq.queue.MessageData
import scala.concurrent.{ Future, Promise }

class Subscriber0(receiveServerAddress: InetSocketAddress)(implicit val system: ActorSystem) extends ReactiveStreamsSupport {

  override def run(): Future[Unit] = {

    system.log.info("Connect to broker: {}", receiveServerAddress)
    val completion = Promise[Unit]()
    val connection = Tcp().outgoingConnection(receiveServerAddress)

    val ps = system.actorOf(Props(new DestinationProcessor(completion)))
    val s = ActorSubscriber[MessageData](ps)
    val p = ActorPublisher[ByteString](ps)

    val reconcileFrames = new Framers()

    val sink = Flow[ByteString].mapConcat(reconcileFrames.apply).map(MessageData.decodeFromString).to(Sink(s))
    val source = Source[ByteString](p)

    val settings = ActorMaterializerSettings(system)
      .withInputBuffer(initialSize = 2, maxSize = 8)
      .withDispatcher("akka.subscriber-dispatcher")

    connection.to(sink)
      .runWith(source)(ActorMaterializer(settings))

    completion.future
  }
}