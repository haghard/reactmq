package com.reactmq.cluster

trait ClusterClientSeedNodeSupport extends SeedNodeSupport {
  override val formatter: (List[String]) ⇒ String =
    seeds ⇒
      seeds.map { node ⇒
        val v = node.split(":")
        s"""cluster.client.initial-contact-points += "akka.tcp://$ActorSystemName@${v(0)}:${v(1)}""""
      }.mkString("\n")

}
