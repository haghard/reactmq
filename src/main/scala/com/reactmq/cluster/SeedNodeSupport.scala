package com.reactmq.cluster

import java.net.{ InetAddress, NetworkInterface }
import scala.collection.JavaConverters._
import scalaz._
import Scalaz._

trait SeedNodeSupport {
  val AKKA_PORT = "AKKA_PORT"
  val SEEDS = "SEEDS"

  val ipExpression = """\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}"""

  def ActorSystemName: String

  def eth: String

  /**
   * Find network address using eth
   * @return
   */
  def localAddresses: Option[InetAddress] =
    NetworkInterface.getNetworkInterfaces.asScala.toList
      .find(_.getName == eth)
      .flatMap(x ⇒ x.getInetAddresses.asScala.toList.find(i ⇒ i.getHostAddress.matches(ipExpression)))

  def validatePort(port: String): ValidationNel[String, Int] = {
    if (port == null) "Port is empty".failureNel
    else {
      try {
        port.toInt.successNel
      } catch {
        case e: Exception ⇒ e.getMessage.failureNel
      }
    }
  }

  def validateSeeds(seeds: String): ValidationNel[String, String] = {
    if (seeds == null) "Seeds is empty".failureNel
    else {
      seeds.split(",").toList.map { node ⇒
        val v = node.split(":")
        s"""akka.cluster.seed-nodes += "akka.tcp://$ActorSystemName@${v(0)}:${v(1)}""""
      }.mkString("\n").successNel
    }
  }

  def validateHost: ValidationNel[String, InetAddress] = localAddresses match {
    case None    ⇒ s"Can't find network by $eth".failureNel
    case Some(a) ⇒ a.successNel
  }

  def validateBroker(port: String, seeds: String): ValidationNel[String, (Int, String, String)] =
    (validatePort(port) |@| validateSeeds(seeds) |@| validateHost) {
      case (p, s, a) ⇒ (p, s, a.getHostAddress)
    }
}
