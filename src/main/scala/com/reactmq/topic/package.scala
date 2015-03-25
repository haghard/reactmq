package com.reactmq

package object topic {

  case class User(id: String = "", name: String = "", screenName: String = "", lang: String = "")

  case class Tweet(id: String = "", text: String = "", user: Option[User] = None,
                   topic: Option[String] = None, createdAt: String = "")

}
