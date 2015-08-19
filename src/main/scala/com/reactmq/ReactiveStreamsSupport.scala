package com.reactmq

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.{ Promise, Future }

trait ReactiveStreamsSupport {
  implicit def system: ActorSystem

  implicit val dispatcher = system.dispatcher

  implicit val timeout = Timeout(5.seconds)

  def materializer = ActorMaterializer()

  def run(): Future[Unit]

  def handleIOFailure(ioFuture: Future[Any], msg: ⇒ String, failPromise: Option[Promise[Unit]] = None) {
    ioFuture.onFailure {
      case e: Exception ⇒
        system.log.info("Critical error {}", e.getMessage)
        failPromise.foreach(_.failure(e))
    }
  }
}
