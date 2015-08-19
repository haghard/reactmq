package com.reactmq.reader

import java.net.InetSocketAddress
import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Tcp, Sink, Source }
import com.reactmq.Framing._
import com.reactmq.cluster.ClusterClientSupport
import com.reactmq.topic.{ User, Tweet }
import com.reactmq.ReactiveStreamsSupport

import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }

object ClusterTopicsPublisher extends App with ClusterClientSupport {
  start("tweet-publisher", (ba, system) ⇒ new TweetPublisher(ba.publishersAddress)(system).run())
}

class TweetPublisher(publisherAddress: InetSocketAddress)(implicit val system: ActorSystem) extends ReactiveStreamsSupport {

  val topics = Vector("cle", "ind")

  def nextChar = (ThreadLocalRandom.current().nextInt(91 - 65) + 65).toChar

  val publisherName = List.fill(5)(nextChar).mkString

  override def run(): Future[Unit] = {
    var idx = 0
    system.log.info("Publisher address {}", publisherAddress)
    val completion = Promise[Unit]()

    val con = Tcp().outgoingConnection(publisherAddress)

    val tweetSource = Source(1.seconds, 1.second, () ⇒ {
      idx += 1;
      Tweet(idx.toString, "tweet body", Some(User(id = publisherName)),
        Some(topics(ThreadLocalRandom.current.nextInt(topics.size))))
    }).map { gen ⇒
      val t = gen()
      system.log.info(s"Publish: $t")
      toBytes(t)
    }

    tweetSource.via(con)
      .runWith(Sink.onComplete(t ⇒ completion.complete(t)))

    completion.future
  }
}

