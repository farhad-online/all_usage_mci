package bigdata.dwbi.mci

import config.ConfigManager

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    ConfigManager.initialize() match {
      case Right(config) =>
        println(s"Successfully loaded configuration for app: ${config.appName}")

        val spark = config.spark.toSparkSession

        config.kafka.foreach { kafkaConfig =>
          val props = kafkaConfig.toKafkaProperties
          println(s"Kafka consumer configured for topics: ${kafkaConfig.topics.mkString(", ")}")
        }

        config.hive.foreach { hiveConfig =>
          val hiveConfigs = hiveConfig.toHiveConfigs
          hiveConfigs.foreach { case (key, value) =>
            spark.conf.set(key, value)
          }
        }

        ConfigManager.subAppConfig("data-processor").foreach { subApp =>
          println(s"Sub-app ${subApp.name} is ${if (subApp.enabled) "enabled" else "disabled"}")
        }

        runApplication(spark)

      case Left(error) =>
        println(s"Failed to load configuration: $error")
        System.exit(1)
    }
  }

  def runApplication(spark: SparkSession): Unit = {
  }
}