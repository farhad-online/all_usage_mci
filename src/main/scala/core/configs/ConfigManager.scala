package bigdata.dwbi.mci
package core.configs

import bigdata.dwbi.mci.core.configs.hive.SparkHiveConfig
import bigdata.dwbi.mci.core.configs.kafka.SparkKafkaConsumerConfig
import bigdata.dwbi.mci.core.configs.spark.SparkConfig
import bigdata.dwbi.mci.core.logger.Logger
import com.typesafe.config.Config

import scala.collection.JavaConverters.asScalaSetConverter

object ConfigManager extends Logger {
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

    SparkConfig(appName, master, checkpointLocation, format, outputMode, batchMode, batchFormat, triggerInterval, options)
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
