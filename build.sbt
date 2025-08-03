ThisBuild / version := "0.0.1"
ThisBuild / scalaVersion := "2.12.15"

val moduleName = "all_usage"
val postgresVersion = "42.2.27"
val hadoopVersion = "3.3.5"
val sparkVersion = "3.2.3"
val kafkaVersion = "2.8.1"
val hiveVersion = "3.1.3"
val log4jVersion = "2.21.1"

lazy val root = (project in file("."))
  .settings(
    name := moduleName,
    idePackagePrefix := Some("bigdata.dwbi.mci")
  )

ThisBuild / packageBin / artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  s"${module.name}-${module.revision}.jar"
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.postgresql" % "postgresql" % postgresVersion,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,
  "com.github.pureconfig" %% "pureconfig" % "0.17.4",
  "com.github.pureconfig" %% "pureconfig-generic" % "0.17.4",
  "com.typesafe" % "config" % "1.4.3"
)
