package com.reactmq.cluster

import java.net.{ InetAddress, NetworkInterface }
import scala.collection.JavaConverters._
import scalaz._
import Scalaz._

trait SeedNodeSupport {
  val AKKA_PORT_VAR = "AKKA_PORT"
  val SEEDS_VAR = "SEEDS"
  val TOPIC_VAR = "TOPIC"

  val ipExpression = """\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}"""

  val ActorSystemName = "Broker"

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

  def formatter: List[String] ⇒ String

  def validateSeeds(seeds: String): ValidationNel[String, String] =
    if (seeds == null) "Seeds is empty".failureNel
    else
      try formatter(seeds.split(",").toList).successNel
      catch {
        case e: Exception ⇒ e.getMessage.failureNel
      }

  def validateHost: ValidationNel[String, InetAddress] = localAddresses match {
    case None    ⇒ s"Can't find network by $eth".failureNel
    case Some(a) ⇒ a.successNel
  }

  def validateTopics(topics: String): ValidationNel[String, Vector[String]] = {
    if (topics == null) "Topics is empty".failureNel
    else topics.split(",").toVector.successNel
  }

  def validate(port: String, seeds: String): ValidationNel[String, (Int, String, String)] =
    (validatePort(port) |@| validateSeeds(seeds) |@| validateHost) {
      case (p, s, a) ⇒ (p, s, a.getHostAddress)
    }

  def validateAll(port: String, seeds: String, topics: String): ValidationNel[String, (Int, String, String, Vector[String])] = {
    (validate(port, seeds) |@| validateTopics(topics)) {
      case (vs, t) ⇒ (vs._1, vs._2, vs._3, t)
    }
  }
}