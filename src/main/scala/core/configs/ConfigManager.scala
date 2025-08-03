package bigdata.dwbi.mci
package core.configs

import bigdata.dwbi.mci.core.configs.apps.main.MainAppConfig
import bigdata.dwbi.mci.core.configs.apps.subapp.SubAppBaseConfig
import bigdata.dwbi.mci.core.configs.hive.SparkHiveConfig
import bigdata.dwbi.mci.core.configs.kafka.SparkKafkaConsumerConfig
import bigdata.dwbi.mci.core.configs.spark.SparkConfig
import bigdata.dwbi.mci.core.logger.Logger
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters.asScalaSetConverter

object ConfigManager extends Logger {

  def loadConfig(configPath: String = "application.conf"): MainAppConfig = {
    val config = ConfigFactory.load(configPath)
    parseMainAppConfig(config)
  }

  private def parseMainAppConfig(config: Config): MainAppConfig = {
    val appName = config.getString("appName")
    val environment = config.getString("environment")
    val version = config.getString("version")

    val subApps = if (config.hasPath("subApps")) {
      config.getConfig("subApps").root().keySet().asScala.map { key =>
        val subAppConfig = config.getConfig(s"subApps.$key")
        key -> parseSubAppBaseConfig(subAppConfig)
      }.toMap
    } else {
      Map.empty[String, SubAppBaseConfig]
    }

    MainAppConfig(appName, environment, version, subApps)
  }

  private def parseSubAppBaseConfig(config: Config): SubAppBaseConfig = {
    val name = config.getString("name")
    val env = config.getString("env")
    val enabled = if (config.hasPath("enabled")) config.getBoolean("enabled") else false


    val spark = if (config.hasPath("spark")) {
      Some(parseSparkConfig(config.getConfig("spark")))
    } else None

    val sparkKafkaConsumer = if (config.hasPath("sparkKafkaConsumer")) {
      Some(parseSparkKafkaConsumerConfig(config.getConfig("sparkKafkaConsumer")))
    } else None

    val hive = if (config.hasPath("hive")) {
      Some(parseSparkHiveConfig(config.getConfig("hive")))
    } else None

    val options = if (config.hasPath("options")) {
      parseStringMap(config.getConfig("options"))
    } else Map.empty[String, String]

    SubAppBaseConfig(name, env, enabled, spark, sparkKafkaConsumer, hive, options)
  }

  def parseSparkConfig(config: Config): SparkConfig = {
    val appName = config.getString("appName")
    val master = config.getString("master")
    val checkpointLocation = config.getString("checkpointLocation")
    val format = config.getString("format")
    val outputMode = config.getString("outputMode")
    val batchMode = config.getString("batchMode")
    val batchFormat = config.getString("batchFormat")
    val triggerInterval = config.getInt("triggerInterval")

    val options = if (config.hasPath("options")) {
      parseStringMap(config.getConfig("options"))
    } else Map.empty[String, String]

    SparkConfig(appName, master, checkpointLocation, format, outputMode, batchFormat, batchFormat, triggerInterval, options)
  }

  def parseSparkKafkaConsumerConfig(config: Config): SparkKafkaConsumerConfig = {
    val format = config.getString("format")
    val options = if (config.hasPath("options")) {
      parseStringMap(config.getConfig("options"))
    } else Map.empty[String, String]

    SparkKafkaConsumerConfig(format, options)
  }

  def parseSparkHiveConfig(config: Config): SparkHiveConfig = {
    val tableName = config.getString("tableName")
    val partitionKey = config.getString("partitionKey")
    val options = if (config.hasPath("options")) {
      parseStringMap(config.getConfig("options"))
    } else Map.empty[String, String]

    SparkHiveConfig(tableName, partitionKey, options)
  }

  def parseStringMap(config: Config): Map[String, String] = {
    config.entrySet().asScala.map { entry =>
      entry.getKey -> entry.getValue.unwrapped().toString
    }.toMap
  }
}
