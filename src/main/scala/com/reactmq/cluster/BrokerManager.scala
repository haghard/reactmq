package com.reactmq.cluster

import akka.actor._
import com.reactmq.Broker
import java.net.InetSocketAddress
import com.reactmq.topic.MultiBroker
import com.typesafe.config.ConfigFactory
import akka.contrib.pattern.{ ClusterReceptionistExtension, ClusterSingletonManager }

object Brokers extends Enumeration {
  type BType = Value
  val Single, Multi = Value
}

class BrokerManager(clusterPort: Int, t: Brokers.BType) {
  def run(): Unit = {
    val conf = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$clusterPort")
      .withFallback(ConfigFactory.load("cluster-broker-template"))

    val system = ActorSystem(s"broker", conf)

    system.actorOf(ClusterSingletonManager.props(
      singletonProps = Props(new BrokerManagerActor(clusterPort, t)),
      singletonName = "broker",
      terminationMessage = PoisonPill,
      role = Some("broker")),
      name = "broker-manager")
  }
}

class BrokerManagerActor(clusterPort: Int, t: Brokers.BType) extends Actor with ActorLogging {
  val sendServerAddress = new InetSocketAddress("localhost", clusterPort + 10)
  val receiveServerAddress = new InetSocketAddress("localhost", clusterPort + 20)

  override def preStart() = {
    super.preStart()
    t match {
      case Brokers.Single ⇒
        new Broker(sendServerAddress, receiveServerAddress)(context.system).run()
      case Brokers.Multi ⇒
        new MultiBroker(sendServerAddress, receiveServerAddress)(context.system).run()
    }

    ClusterReceptionistExtension(context.system).registerService(self)
  }

  override def receive = {
    case GetBrokerAddresses ⇒
      log.info("GetBrokerAddresses from {}", sender())
      sender() ! BrokerAddresses(sendServerAddress, receiveServerAddress)
  }
}

case object GetBrokerAddresses
case class BrokerAddresses(publishersAddress: InetSocketAddress, subscribersAddress: InetSocketAddress)

object ClusteredBroker1 extends App {
  new BrokerManager(9171, Brokers.Single).run()
}

object ClusteredBroker2 extends App {
  new BrokerManager(9172, Brokers.Single).run()
}

/************************************************/
object ClusteredTopicsBroker1 extends App {
  new BrokerManager(9171, Brokers.Multi).run()
}

object ClusteredTopicsBroker2 extends App {
  new BrokerManager(9172, Brokers.Multi).run()
}