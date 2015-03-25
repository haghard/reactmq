package com.reactmq.cluster

import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import akka.contrib.pattern.ClusterClient
import akka.actor.{ ActorSystem, AddressFromURIString, RootActorPath }

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

trait ClusterClientSupport {

  implicit val timeout = Timeout(10.seconds)

  def start(name: String, runClient: (BrokerAddresses, ActorSystem) ⇒ Future[Unit]) {
    val conf = ConfigFactory.load("cluster-client")
    implicit val system = ActorSystem(name, conf)
    import system.dispatcher

    val initialContacts = conf.getStringList("cluster.client.initial-contact-points").asScala.map {
      case AddressFromURIString(addr) ⇒ system.actorSelection(RootActorPath(addr) / "user" / "receptionist")
    }.toSet
    system.log.info(s"Initial cluster contact: $initialContacts")

    val clusterClient = system.actorOf(ClusterClient.props(initialContacts), "cluster-client")

    def go(): Unit = {
      val completionFuture = (clusterClient ? ClusterClient.Send("/user/broker-manager/broker", GetBrokerAddresses, localAffinity = false))
        .mapTo[BrokerAddresses]
        .flatMap { ba ⇒
          system.log.info(s"Connecting a $name using broker address $ba.")
          runClient(ba, system)
        }

      completionFuture.onComplete { result ⇒
        system.log.info(s"$name completed with result $result. Scheduling restart .")
        system.scheduler.scheduleOnce(5.second, new Runnable {
          override def run() = go()
        })
      }
    }

    go()
  }
}
