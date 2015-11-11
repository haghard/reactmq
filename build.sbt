import com.scalapenos.sbt.prompt.SbtPrompt.autoImport._
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import sbt.Keys._
import sbt._

import scalariform.formatter.preferences._

organization  := "com.haghard"
name := "reactmq"
version := "0.1"

scalaVersion := "2.11.7"
parallelExecution in Test := false
promptTheme := Scalapenos

val Akka = "2.4.0"
val AkkaStreamsVersion = "2.0-M1"
val localMvnRepo = "/Volumes/Data/dev_build_tools/apache-maven-3.1.1/repository"
val ivy = "~/.ivy2/local/"

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(RewriteArrowSymbols, true)
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers += "Sonatype Snapshots Repo"  at "https://oss.sonatype.org/content/groups/public"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
resolvers += "Local Maven Repository" at "file:///" + localMvnRepo
resolvers += "Local Ivy Repository" at "file:///" + ivy

scalacOptions in Compile ++= Seq(
  "-feature",
  "-deprecation",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps",
  "-language:reflectiveCalls",
  "-encoding", "UTF-8",
  "-target:jvm-1.6",
  "-feature", "-unchecked",
  "-Xlog-reflective-calls",
  "-Xlint"
)

javacOptions in Compile ++= Seq("-source", "1.6", "-target", "1.6", "-Xlint:unchecked", "-Xlint:deprecation")

libraryDependencies ++= Seq(
  "com.typesafe.akka"       %%    "akka-actor"                  % Akka,
  "com.typesafe.akka"       %%    "akka-cluster-sharding"       %  Akka withSources(),
  "com.typesafe.akka"       %%    "akka-cluster-tools"          %  Akka withSources(),
  "com.typesafe.akka"       %%    "akka-persistence"            % Akka withSources() intransitive(),
  "com.github.krasserm"     %%    "akka-persistence-cassandra"  % "0.4",
  "com.typesafe.akka"       %%    "akka-stream-experimental"    % AkkaStreamsVersion,
  "io.spray"                %%    "spray-json"                  % "1.2.6",
  "org.scalaz"              %%    "scalaz-core"                 % "7.1.4",
  "org.scalatest"           %%    "scalatest"                   % "2.2.2" % "test",
  "joda-time"               %     "joda-time"                   % "2.5",
  "org.joda"                %     "joda-convert"                % "1.7"
)

libraryDependencies ++= Seq(
  "org.slf4j"               %   "slf4j-api"       % "1.7.7",
  "ch.qos.logback"          %   "logback-classic" % "1.1.2",
  "com.typesafe.akka"       %%  "akka-slf4j"      % Akka
)

publishMavenStyle := true
publishTo := Some(Resolver.file("file",  new File(localMvnRepo)))
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

val cassandra = "192.168.0.134"

addCommandAlias("broker1", "run-main com.reactmq.cluster.ClusteredTopicsBroker --TOPIC=hou,okc,mia,cle --AKKA_PORT=2551 --SEEDS=192.168.0.62:2551,192.168.0.62:2552 --DB_HOSTS=" + cassandra)
addCommandAlias("broker2", "run-main com.reactmq.cluster.ClusteredTopicsBroker --TOPIC=hou,okc,mia,cle --AKKA_PORT=2552 --SEEDS=192.168.0.62:2551,192.168.0.62:2552 --DB_HOSTS=" + cassandra)

addCommandAlias("pub", "run-main com.reactmq.reader.ClusterTopicsPublisher --TOPIC=hou,okc,mia --AKKA_PORT=2554 --SEEDS=192.168.0.62:2551,192.168.0.62:2552")

//all topics
addCommandAlias("sub", "run-main com.reactmq.cluster.ClusterTopicsSubscriber --AKKA_PORT=2556 --SEEDS=192.168.0.62:2551,192.168.0.62:2552")