package com.reactmq.cluster

import scala.collection._

trait SystemPropsSupport {

  val opt = """--(\S+)=(\S+)""".r

  private def argsToProps(args: Array[String]) =
    args.collect { case opt(key, value) ⇒ key -> value }(breakOut)

  def applySystemProperties(args: Array[String]) = {
    for ((key, value) ← argsToProps(args)) {
      println(s"set SYSTEM_PROPERTY: $key - $value")
      System.setProperty(key, value)
    }
  }
}
