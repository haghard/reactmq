import com.scalapenos.sbt.prompt.SbtPrompt.autoImport._
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import sbt.Keys._
import sbt._

import scalariform.formatter.preferences._

organization  := "com.haghard"
name := "reactmq"
version := "0.1"

scalaVersion := "2.11.4"
parallelExecution in Test := false
promptTheme := Scalapenos

val akkaVersion = "2.3.7"
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
  // akka
  "com.typesafe.akka"   %% "akka-actor"                     % akkaVersion,
  "com.typesafe.akka"   %% "akka-persistence-experimental"  % akkaVersion,
  "com.typesafe.akka"   %% "akka-cluster"                   % akkaVersion,
  "com.typesafe.akka"   %% "akka-contrib"                   % akkaVersion,
  "com.typesafe.akka"   %% "akka-stream-experimental"       % "0.11",
  "com.github.ddevore"  %% "akka-persistence-mongo-casbah"  % "0.7.4",
  "com.typesafe.akka"   %%  "akka-slf4j"                    % akkaVersion,
  "io.spray"            %%  "spray-json"                    % "1.2.6",

  // util
  "org.scalatest" %% "scalatest" % "2.2.2" % "test",
  "joda-time" % "joda-time" % "2.5",
  "org.joda" % "joda-convert" % "1.7"
)

libraryDependencies ++= Seq(
  "org.slf4j"               %   "slf4j-api"       % "1.7.7",
  "ch.qos.logback"          %   "logback-core"    % "1.1.2",
  "ch.qos.logback"          %   "logback-classic" % "1.1.2"
)

publishMavenStyle := true
publishTo := Some(Resolver.file("file",  new File(localMvnRepo)))
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }