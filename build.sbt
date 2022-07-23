import sbtassembly.AssemblyPlugin.assemblySettings

// Your sbt build file. Guides on how to write one can be found at
// http://www.scala-sbt.org/0.13/docs/index.html
ThisBuild / scalaVersion := "2.12.4"

//val sparkVersion = "3.2.1"
val sparkVersion = "3.2.1-next"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "org.apache.spark" %% "spark-hive-thriftserver" % sparkVersion,
)

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.2.0-next"

libraryDependencies += "io.fabric8" % "kubernetes-client" % "5.4.1"

libraryDependencies += "dnsjava" % "dnsjava" % "3.0.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-auth" % "3.3.1"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"

libraryDependencies += "com.twitter" %% "util-jvm" % "6.45.0" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

libraryDependencies += "org.yaml" % "snakeyaml" % "1.23"

libraryDependencies += "joda-time" % "joda-time" % "2.10.10"


lazy val root = (project in file("."))
  .settings(assemblySettings)
//  .settings(CopyDependencies.settings)
  .settings(
    name := "spark-sql-perf",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case PathList("org", "apache", "hadoop", xs @ _*) => MergeStrategy.last
      case x => MergeStrategy.first
    }
  )