package com.reactmq.reader

import akka.io.IO
import akka.pattern.ask
import akka.util.ByteString
import com.reactmq.Framing._
import akka.actor.ActorSystem
import akka.stream.io.StreamTcp
import scala.concurrent.duration._
import java.net.InetSocketAddress
import scala.concurrent.{ Future, Promise }
import java.util.concurrent.ThreadLocalRandom
import com.reactmq.cluster.ClusterClientSupport
import akka.stream.scaladsl.{ OnCompleteSink, Source, Sink }
import com.reactmq.{ LocalServerSupport, ReactiveStreamsSupport }

object LocalSender extends App with LocalServerSupport {
  new Sender(sendServerAddress).run()
}

object ClusterSender extends App with ClusterClientSupport {
  start("sender", (ba, system) ⇒ new Sender(ba.sendServerAddress)(system).run())
}

class Sender(sendServerAddress: InetSocketAddress)(implicit val system: ActorSystem)
    extends ReactiveStreamsSupport {

  def run(): Future[Unit] = {

    val completionPromise = Promise[Unit]()

    system.log.info("Sender address {}", sendServerAddress)

    val connectFuture = IO(StreamTcp) ? StreamTcp.Connect(sendServerAddress)

    connectFuture.onSuccess {
      case binding: StreamTcp.OutgoingTcpConnection ⇒
        system.log.info("Sender: connected to broker")

        def nextChar = (ThreadLocalRandom.current().nextInt(91 - 65) + 65).toChar
        val senderName = List.fill(5)(nextChar).mkString
        var idx = 0

        Source(1.second, 1.second, () ⇒ { idx += 1; s"Message $idx from $senderName" })
          .map { msg ⇒
            system.log.info(s"Sender: sending $msg")
            createFrame(msg)
          }
          .runWith(Sink(binding.outputStream))

        Source(binding.inputStream)
          .runWith(OnCompleteSink[ByteString] { t ⇒
            completionPromise.complete(t); ()
          })
    }

    handleIOFailure(connectFuture, "Sender: failed to connect to broker", Some(completionPromise))

    completionPromise.future
  }
}