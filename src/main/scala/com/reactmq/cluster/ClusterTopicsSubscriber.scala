package com.reactmq.cluster

import akka.util.ByteString
import com.reactmq.topic.Tweet
import java.net.InetSocketAddress
import scala.concurrent.{ Promise, Future }
import akka.actor.ActorSystem
import akka.stream.actor.{ ActorPublisher, ActorSubscriber }
import akka.stream.scaladsl.{ Source, Sink, Flow, Tcp }
import com.reactmq.{ ConfirmationProcessor, Framers, ReactiveStreamsSupport }
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }

object ClusterTopicsSubscriber extends App with ClusterClientSupport
    with ClusterClientSeedNodeSupport with SystemPropsSupport {

  override def eth: String = "en0"

  if (!args.isEmpty)
    applySystemProperties(args)

  validate(System.getProperty(AKKA_PORT_VAR), System.getProperty(SEEDS_VAR))
    .fold(errors ⇒ throw new Exception(errors.toString()), { v ⇒
      val akkaPort = v._1
      val contactPoints = v._2
      val hostName = v._3

      start("topic-subscriber",
        (ba, system) ⇒ new TopicsSubscriber(ba.subscribersAddress)(system).run(),
        akkaPort, contactPoints, hostName)
    })
}

/**
 * Can receive duplicates, at-least-once delivery
 *
 * @param receiveServerAddress
 * @param system
 */
final class TopicsSubscriber(receiveServerAddress: InetSocketAddress)(implicit val system: ActorSystem) extends ReactiveStreamsSupport {
  val name = "akka.client-dispatcher"

  override val materializer = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withInputBuffer(initialSize = 4, maxSize = 16)
      .withDispatcher(name))

  override def run(): Future[Unit] = {
    val completion = Promise[Unit]()
    val connection = Tcp().outgoingConnection(receiveServerAddress)

    val processor = system.actorOf(ConfirmationProcessor.prop(completion))
    val s = ActorSubscriber[Tweet](processor)
    val p = ActorPublisher[ByteString](processor)

    val reconcileFrames = new Framers()

    val sink = Flow[ByteString].mapConcat(reconcileFrames.apply2).to(Sink(s))
    val source = Source[ByteString](p)

    connection
      .runWith(source, sink)(materializer)
    completion.future
  }
}