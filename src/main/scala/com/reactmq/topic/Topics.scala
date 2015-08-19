package com.reactmq.topic

import akka.actor.{ ActorLogging, Props }
import akka.persistence.PersistentActor
import com.reactmq.util.NowProvider
import spray.json._

object Topics {

  sealed trait TopicEvent {
    def id: String
  }

  case class MessageData(id: String, tweet: Tweet) {

    def encodeAsString = JsObject(
      "id" -> JsString(tweet.id),
      "user" -> JsObject("id" -> JsString(tweet.user.map(_.id).getOrElse("none")),
        "name" -> JsString(tweet.user.map(_.name).getOrElse("none")),
        "screen_name" -> JsString(tweet.user.map(_.screenName).getOrElse("none")),
        "lang" -> JsString(tweet.user.map(_.lang).getOrElse("none"))),
      "body" -> JsString(tweet.text),
      "createdAt" -> JsString(tweet.createdAt),
      "topic" -> JsString(tweet.topic.getOrElse("none"))).prettyPrint
  }

  case class MessageAdded(id: String, nextDelivery: Long, t: Tweet) extends TopicEvent
  case class MessageNextDeliveryUpdated(id: String, nextDelivery: Long) extends TopicEvent
  case class MessageDeleted(id: String) extends TopicEvent

  case class SaveTopicMessage(t: Tweet)
  case class ReceiveTopicMessages(topic: String, count: Int)
  case class ConfirmTopicMessage(id: String)

  // replies
  case class SentTopicMessage(id: String)
  case class ReceivedTopicMessages(msg: List[MessageData])

  def props(teams: List[String], twitterTeams: Map[String, String]) =
    Props(new Topics(teams, twitterTeams))
      .withDispatcher("akka.topics-dispatcher")
}

class Topics(val teams: List[String], val twitterTeams: Map[String, String]) extends PersistentActor with ActorLogging
    with TopicsStorage
    with TopicReceive
    with TopicsRecover {

  override def nowProvider = new NowProvider()

  override def persistenceId = s"${self.path.parent.name}-${self.path.name}"

  override def receiveCommand = handleCommands

  override def receiveRecover = handleRecover
}
