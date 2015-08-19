package com.reactmq.cluster

import akka.actor._
import com.reactmq.Broker
import java.net.{ InetSocketAddress }
import com.reactmq.topic.MultiTopicBroker
import com.typesafe.config.ConfigFactory
import akka.contrib.pattern.{ ClusterReceptionistExtension, ClusterSingletonManager }

object Brokers extends Enumeration {
  type BType = Value
  val Single, Multi = Value
}

class BrokerBootstrap(akkaPort: Int, seeds: String, hostName: String, t: Brokers.BType, systemName: String) {
  val ClusterRole = "brokers"
  def runAkka() = {
    val clusterCfg = ConfigFactory.empty()
      .withFallback((ConfigFactory parseString seeds).resolve())
      .withFallback(ConfigFactory.parseString(s"""
        |akka.cluster {
        |    auto-down-unreachable-after = 10s
        |    roles = [ $ClusterRole ]
        |  }
      """.stripMargin))

    val conf = ConfigFactory.empty().withFallback(clusterCfg)
      .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$akkaPort"))
      .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname=$hostName"))
      .withFallback(ConfigFactory.load("cluster-broker.conf"))

    val brokerCfg = conf.getConfig(systemName)
    val inPort = brokerCfg.getInt("in")
    val outPort = brokerCfg.getInt("out")

    val system = ActorSystem(systemName, conf)

    val message = new StringBuilder().append('\n')
      .append("=====================================================================================================================================")
      .append('\n')
      .append(s"★ ★ ★ ★ ★ ★  Cluster role: $ClusterRole - Akka-System: $hostName:$akkaPort  ★ ★ ★ ★ ★ ★")
      .append('\n')
      .append(s"★ ★ ★ ★ ★ ★  Akka cluster seed nodes: ${system.settings.config.getStringList("akka.cluster.seed-nodes")}  ★ ★ ★ ★ ★ ★")
      .append('\n')
      .append(s"★ ★ ★ ★ ★ ★  Broker in: $hostName:$inPort - out: $hostName:$outPort  ★ ★ ★ ★ ★ ★").append('\n')
      .append("=====================================================================================================================================")
      .append('\n')
      .toString

    system.log.info(message)

    system.actorOf(ClusterSingletonManager.props(
      singletonProps = Props(new BrokerGuard(new InetSocketAddress(hostName, inPort), new InetSocketAddress(hostName, outPort), t)),
      singletonName = "broker",
      terminationMessage = PoisonPill,
      role = Some(ClusterRole)),
      name = "broker-guard")
  }
}

class BrokerGuard(in: InetSocketAddress, out: InetSocketAddress, t: Brokers.BType) extends Actor with ActorLogging {
  override def preStart() = {
    t match {
      case Brokers.Single ⇒ new Broker(in, out)(context.system).run()
      case Brokers.Multi  ⇒ new MultiTopicBroker(in, out)(context.system).run()
    }

    ClusterReceptionistExtension(context.system).registerService(self)
  }

  override def receive = {
    case GetBrokerAddresses ⇒
      log.info("GetBrokerAddresses from {}", sender())
      sender() ! BrokerAddresses(in, out)
  }
}

case object GetBrokerAddresses
case class BrokerAddresses(publishersAddress: InetSocketAddress, subscribersAddress: InetSocketAddress)

object ClusteredTopicsBroker extends App with SeedNodeSupport with SystemPropsSupport {

  override val eth = "en0"

  override val ActorSystemName = "Broker"

  if (!args.isEmpty)
    applySystemProperties(args)

  validateBroker(System.getProperty(AKKA_PORT), System.getProperty(SEEDS))
    .fold(errors ⇒ throw new Exception(errors.toString()), { v ⇒
      new BrokerBootstrap(v._1, v._2, v._3, Brokers.Multi, ActorSystemName).runAkka()
    })
}