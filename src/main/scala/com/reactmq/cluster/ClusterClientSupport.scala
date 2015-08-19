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

  implicit val reconnectionTimeout = Timeout(8.seconds)

  def start(name: String, runClient: (BrokerAddresses, ActorSystem) ⇒ Future[Unit],
            akkaPort: Int, contactPoints: String, hostName: String) {

    val conf = ConfigFactory.empty()
      .withFallback((ConfigFactory parseString contactPoints).resolve())
      .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$akkaPort"))
      .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname=$hostName"))
      .withFallback(ConfigFactory.load("cluster-client"))

    val system = ActorSystem(name, conf)
    implicit val EC = system.dispatchers.lookup("client-dispatcher")

    val message = new StringBuilder().append('\n')
      .append("=====================================================================================================================================")
      .append('\n')
      .append(s"★ ★ ★ ★ ★ ★  Akka-System: $hostName:$akkaPort  ★ ★ ★ ★ ★ ★")
      .append('\n')
      .append(s"★ ★ ★ ★ ★ ★  Broker contact points: ${system.settings.config.getStringList("cluster.client.initial-contact-points")}  ★ ★ ★ ★ ★ ★")
      .append('\n')
      .append("=====================================================================================================================================")
      .append('\n')
      .toString

    system.log.info(message)

    val initialContacts = conf.getStringList("cluster.client.initial-contact-points").asScala.map {
      case AddressFromURIString(addr) ⇒ system.actorSelection(RootActorPath(addr) / "user" / "receptionist")
    }.toSet

    val clusterClient = system.actorOf(ClusterClient.props(initialContacts), "cluster-client")

    def go(): Unit = {
      val clientFlow = (clusterClient ? ClusterClient.Send("/user/broker-guard/broker", GetBrokerAddresses, localAffinity = false))
        .mapTo[BrokerAddresses]
        .flatMap { ba ⇒
          system.log.info(s"Connecting a $name using broker address $ba.")
          runClient(ba, system)
        }

      clientFlow.onComplete { result ⇒
        system.log.info(s"$name completed with result $result. Scheduling restart .")
        system.scheduler.scheduleOnce(5.second, new Runnable {
          override def run() = go()
        })
      }
    }
    go()
  }
}