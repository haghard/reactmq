package com.reactmq.reader

import java.net.InetSocketAddress
import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.stream.io.StreamTcp
import akka.stream.scaladsl.{ OnCompleteSink, Sink, Source }
import akka.util.ByteString
import com.reactmq.Framing._
import com.reactmq.topic.{ User, Tweet }
import com.reactmq.{ LocalServerSupport, ReactiveStreamsSupport }

import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }

object LocalTweetSender extends App with LocalServerSupport {
  new TweetSender(sendServerAddress).run()
}

class TweetSender(sendServerAddress: InetSocketAddress)(implicit val system: ActorSystem)
    extends ReactiveStreamsSupport {

  val topics = Vector("cle", "ind", "mia")

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

        Source(1.second, 1.second, () ⇒ {
          idx += 1;
          Tweet(id = idx.toString, text = "tweet body",
            user = Some(User(id = senderName)),
            topic = Some(topics(ThreadLocalRandom.current.nextInt(topics.size))))
        })
          .map { msg ⇒
            system.log.info(s"Sender: sending $msg")
            toBytes(msg)
          }.runWith(Sink(binding.outputStream))

        Source(binding.inputStream)
          .runWith(OnCompleteSink[ByteString] { t ⇒
            completionPromise.complete(t); ()
          })
    }

    handleIOFailure(connectFuture, "Sender: failed to connect to broker", Some(completionPromise))

    completionPromise.future
  }
}

