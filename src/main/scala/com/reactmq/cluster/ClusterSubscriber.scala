
package com.reactmq.cluster

import com.reactmq.Subscriber0

object ClusterSubscriber extends App with ClusterClientSupport {
  start("subscriber", (ba, system) ⇒ new Subscriber0(ba.subscribersAddress)(system).run())
}

