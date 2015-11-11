package com.reactmq.reader

import java.net.InetSocketAddress
import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializerSettings, ActorMaterializer }
import akka.stream.scaladsl.{ Tcp, Sink, Source }
import akka.util.ByteString
import com.reactmq.Framing._
import com.reactmq.topic.{ User, Tweet }
import com.reactmq.ReactiveStreamsSupport
import com.reactmq.cluster.{ ClusterClientSeedNodeSupport, SystemPropsSupport, ClusterClientSupport }

import scala.concurrent.duration._
import scala.concurrent.{ Future, Promise }

object ClusterTopicsPublisher extends App with ClusterClientSupport
    with ClusterClientSeedNodeSupport
    with SystemPropsSupport {

  override def eth: String = "en0"

  if (!args.isEmpty)
    applySystemProperties(args)

  validateAll(System.getProperty(AKKA_PORT_VAR), System.getProperty(SEEDS_VAR), System.getProperty(TOPIC_VAR), System.getProperty(DB_HOSTS))
    .fold(errors ⇒ throw new Exception(errors.toString()), { v ⇒
      val akkaPort = v._1
      val contactPoints = v._2
      val hostName = v._3
      val topics = v._4

      start("tweet-publisher",
        (ba, system) ⇒ new TweetPublisher(ba.publishersAddress, topics)(system).run(),
        akkaPort, contactPoints, hostName)
    })
}

class TweetPublisher(publisherAddress: InetSocketAddress, topics: Vector[String])(implicit val system: ActorSystem) extends ReactiveStreamsSupport {

  val name = "akka.client-dispatcher"

  override val materializer = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withInputBuffer(initialSize = 4, maxSize = 16)
      .withDispatcher(name))

  def nextChar = (ThreadLocalRandom.current().nextInt(91 - 65) + 65).toChar

  val publisherName = List.fill(5)(nextChar).mkString

  override def run(): Future[Unit] = {
    var idx = 0
    system.log.info("Publisher address {}", publisherAddress)
    val completion = Promise[Unit]()

    val con = Tcp().outgoingConnection(publisherAddress)

    /*val twitterSource = Source.tick(1.seconds, 10.millisecond, () ⇒ {
      idx += 1
      Tweet(idx.toString, "tweet body", Some(User(id = publisherName)), Some(topics(ThreadLocalRandom.current.nextInt(topics.size))))
    }).map { gen ⇒
      val t = gen()
      system.log.info(s"Publish: $t")
      toBytes(t)
    }*/

    val twitterSource = Source(() ⇒ new Iterator[ByteString] {
      override def hasNext: Boolean = true
      override def next(): ByteString = {
        idx += 1
        toBytes(Tweet(idx.toString, "tweet body", Some(User(id = publisherName)), Some(topics(ThreadLocalRandom.current.nextInt(topics.size)))))
      }
    })

    (twitterSource via con)
      .runWith(Sink.onComplete(t ⇒ completion.complete(t)))(materializer)

    completion.future
  }
}

