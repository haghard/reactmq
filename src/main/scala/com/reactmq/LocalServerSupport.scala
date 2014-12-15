package com.reactmq

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

trait LocalServerSupport {

  implicit val system = ActorSystem("local-broker", ConfigFactory.parseString("""
      |akka.persistence {
      |    journal.plugin = "casbah-journal"
      |    snapshot-store.plugin = "casbah-snapshot-store"
      |    view.auto-update-interval = 5s
      |  }
      | casbah-journal.mongo-journal-url = "mongodb://192.168.0.143:27017/broker.journal"
      | casbah-snapshot-store.mongo-snapshot-url = "mongodb://192.168.0.143:27017/broker.snapshots"
      | casbah-journal.mongo-journal-write-concern = "acknowledged"
      | casbah-journal.mongo-journal-write-concern-timeout = 5000
    """.stripMargin).withFallback(ConfigFactory.load))

  val sendServerAddress = new InetSocketAddress("localhost", 9182)
  val receiveServerAddress = new InetSocketAddress("localhost", 9183)
}
