package com.reactmq.cluster

import akka.actor._
import akka.cluster.client.ClusterClientReceptionist
import akka.cluster.singleton.{ ClusterSingletonManagerSettings, ClusterSingletonManager }
import com.reactmq.Broker
import java.net.InetSocketAddress
import com.reactmq.topic.MultiTopicBroker
import com.typesafe.config.ConfigFactory

object Brokers extends Enumeration {
  type BType = Value
  val Single, Multi = Value
}

class BrokerBootstrap(akkaPort: Int, seeds: String, hostName: String, topics: List[String], t: Brokers.BType, systemName: String) {
  val ClusterRole = "brokers"

  def run() = {

    val env = ConfigFactory.load("internal.conf")
    val cassandraEPs = env.getConfig("db.cassandra").getString("seeds")
    val cassandraPort = env.getConfig("db.cassandra").getString("port")
    val cassandraContactPoints = cassandraEPs.split(",").map(_.trim).mkString("\"", "\",\"", "\"")
    val seedsLine = (ConfigFactory parseString seeds).resolve()

    val conf = ConfigFactory.empty()
      .withFallback(seedsLine)
      .withFallback(ConfigFactory.parseString(s"""akka.cluster.role { $ClusterRole.min-nr-of-members = 2 }"""))
      .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$akkaPort"))
      .withFallback(ConfigFactory.parseString(s"akka.remote.netty.tcp.hostname=$hostName"))
      .withFallback(ConfigFactory.parseString(s"akka.cluster.roles = [${ClusterRole}]"))
      .withFallback(ConfigFactory.parseString(s"""
        akka.cluster.role {
          $ClusterRole.min-nr-of-members = 2
        }
       """))
      .withFallback(ConfigFactory.parseString(s"cassandra-journal.contact-points=[$cassandraContactPoints]"))
      .withFallback(ConfigFactory.parseString(s"cassandra-snapshot-store.contact-points=[$cassandraContactPoints]"))
      .withFallback(ConfigFactory.parseString(s"cassandra-journal.port=$cassandraPort"))
      .withFallback(ConfigFactory.parseString(s"cassandra-snapshot-store.port=$cassandraPort"))
      .withFallback(ConfigFactory.load("cluster-broker.conf"))

    val system = ActorSystem(systemName, conf)
    val inPort = system.settings.config.getInt("Broker.in")
    val outPort = system.settings.config.getInt("Broker.out")

    val message = new StringBuilder().append('\n')
      .append("=====================================================================================================================================")
      .append('\n')
      .append(s"★ ★ ★ ★ ★ ★  $ClusterRole with topics: $topics - Akka-System: $hostName:$akkaPort  ★ ★ ★ ★ ★ ★")
      .append('\n')
      .append(s"★ ★ ★ ★ ★ ★  Akka cluster seed nodes: ${system.settings.config.getStringList("akka.cluster.seed-nodes")}  ★ ★ ★ ★ ★ ★")
      .append('\n')
      .append(s"★ ★ ★  Broker: [in - $hostName:${system.settings.config.getInt("Broker.in")}] [out - $hostName:${system.settings.config.getInt("Broker.out")}] ★ ★ ★").append('\n')
      .append('\n')
      .append(s"★ ★ ★ ★ ★ ★  Cassandra contact points: ${system.settings.config.getStringList("cassandra-journal.contact-points")}  ★ ★ ★ ★ ★ ★")
      .append('\n')
      .append(s"★ ★ ★ ★ ★ ★ Cassandra max-partition-size: ${system.settings.config.getInt("cassandra-journal.max-partition-size")} ★ ★ ★ ★ ★ ★")
      .append('\n')
      .append("=====================================================================================================================================")
      .toString

    system.log.info(message)

    import scala.concurrent.duration._
    system.actorOf(
      ClusterSingletonManager.props(
        singletonProps = Props(new BrokerGuard(new InetSocketAddress(hostName, inPort), new InetSocketAddress(hostName, outPort), topics, t)),
        terminationMessage = PoisonPill,
        settings = new ClusterSingletonManagerSettings("broker", Some(ClusterRole), 10 seconds, 10 seconds)),
      name = "broker-guard"
    )
  }
}

class BrokerGuard(in: InetSocketAddress, out: InetSocketAddress, topics: List[String], bType: Brokers.BType) extends Actor with ActorLogging {
  override def preStart() = {
    bType match {
      case Brokers.Single ⇒ new Broker(in, out)(context.system).run()
      case Brokers.Multi  ⇒ new MultiTopicBroker(in, out, topics)(context.system).run()
    }

    ClusterClientReceptionist(context.system).registerService(self)
  }

  override def receive = {
    case GetBrokerAddresses ⇒
      log.info("GetBrokerAddresses from {}", sender())
      sender() ! BrokerAddresses(in, out)
  }
}

case object GetBrokerAddresses
case class BrokerAddresses(publishersAddress: InetSocketAddress, subscribersAddress: InetSocketAddress)

trait BrokerSeedsSupport extends SeedNodeSupport {
  override def formatter: List[String] ⇒ String =
    seeds ⇒
      seeds.map { node ⇒
        val v = node.split(":")
        s"""akka.cluster.seed-nodes += "akka.tcp://$ActorSystemName@${v(0)}:${v(1)}""""
      }.mkString("\n")
}

object ClusteredTopicsBroker extends App with BrokerSeedsSupport with SystemPropsSupport {
  override val eth = "en0"

  if (!args.isEmpty)
    applySystemProperties(args)

  validateAll(System.getProperty(AKKA_PORT_VAR), System.getProperty(SEEDS_VAR), System.getProperty(TOPIC_VAR), System.getProperty(DB_HOSTS))
    .fold(errors ⇒ throw new Exception(errors.toString()), { v ⇒
      new BrokerBootstrap(v._1, v._2, v._3, v._4.toList, Brokers.Multi, ActorSystemName).run()
    })
}