package com.reactmq

import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.{ Promise, Future }

trait ReactiveStreamsSupport {
  implicit def system: ActorSystem

  implicit val dispatcher = system.dispatcher

  implicit val timeout = Timeout(5.seconds)

  implicit val materializer = FlowMaterializer()

  def handleIOFailure(ioFuture: Future[Any], msg: ⇒ String, failPromise: Option[Promise[Unit]] = None) {
    ioFuture.onFailure {
      case e: Exception ⇒
        system.log.info("IO error {}", e.getMessage)
        failPromise.foreach(_.failure(e))
    }
  }
}
