import sbt._
import scalapb.compiler.Version.scalapbVersion

object Libs {
  val ScalaVersion = "2.13.0"
  val `jgroup` = "org.jgroups" % "jgroups" % "4.0.0.Final"
  val `zookeeper` =  "org.apache.zookeeper" % "zookeeper" % "3.5.5"
  val `zkclient` = "com.101tec" % "zkclient" % "0.11"
  val `yammer` = "com.yammer.metrics" % "metrics-core" % "2.2.0"
  val `scalaLogging` = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  val `jacksonDatabind` =  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.9.3"
  val `jacksonDataformatCsv` =  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv" % "2.9.9"
  val `jacksonModuleScala` =  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9"
  val `jacksonJDK8Datatypes` =  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % "2.9.9"
  val `jacksonJaxrsJsonProvider` = "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-json-provider" % "2.9.9"
  val `scalaCollectionCompat` = "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.2"
  val `scalaTest` = "org.scalatest" %% "scalatest" % "3.0.8"
  val `googleGuava` = "com.google.guava" % "guava" % "23.0"
  val `jamm` = "com.github.jbellis" % "jamm" % "0.3.3"
  val `mockito` = "org.mockito" %% "mockito-scala" % "1.5.16"
  val `pcj` = "pcj" % "pcj" % "1.2"
}
