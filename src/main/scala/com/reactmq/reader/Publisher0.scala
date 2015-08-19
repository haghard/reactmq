package com.reactmq.reader

import com.reactmq.Framing._
import akka.actor.ActorSystem
import scala.concurrent.duration._
import java.net.InetSocketAddress
import scala.concurrent.{ Future, Promise }
import java.util.concurrent.ThreadLocalRandom
import com.reactmq.cluster.ClusterClientSupport
import akka.stream.scaladsl._
import com.reactmq.ReactiveStreamsSupport

object ClusterPublisher extends App with ClusterClientSupport {
  start("publisher", (ba, system) ⇒ new Publisher0(ba.publishersAddress)(system).run())
}

class Publisher0(sendServerAddress: InetSocketAddress)(implicit val system: ActorSystem) extends ReactiveStreamsSupport {

  override def run(): Future[Unit] = {
    var idx = 0
    val completion = Promise[Unit]()
    def nextChar() = (ThreadLocalRandom.current().nextInt(91 - 65) + 65).toChar
    val publisherName = List.fill(5)(nextChar).mkString

    val con = Tcp().outgoingConnection(sendServerAddress)

    val srcGen = Source(1.second, 1.second, () ⇒ { idx += 1; s"Message $idx from $publisherName" }) map { msg ⇒
      val m = msg()
      system.log.info(s"Publisher: sending $m")
      createFrame(m)
    }

    srcGen.via(con).runWith(Sink.onComplete(t ⇒ completion.complete(t)))

    system.log.info("Sender address {}", sendServerAddress)
    completion.future
  }
}