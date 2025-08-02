package bigdata.dwbi.mci
package core.configs

import core.configs.apps.main.MainAppConfig
import core.logger.Logger
import core.configs.hive.HiveConfig
import core.configs.kafka.SparkKafkaConsumerConfig
import core.configs.spark.SparkConfig
import core.configs.apps.subapp.SubAppBaseConfig

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters.asScalaSetConverter

import scala.util.{Failure, Success, Try}

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
      Some(parseHiveConfig(config.getConfig("hive")))
    } else None
    
    val options = if (config.hasPath("options")) {
      parseStringMap(config.getConfig("options"))
    } else Map.empty[String, String]
    
    SubAppBaseConfig(name, env, enabled, spark, sparkKafkaConsumer, hive, options)
  }

  private def parseSparkConfig(config: Config): SparkConfig = {
    val appName = config.getString("appName")
    val master = config.getString("master")
    
    val options = if (config.hasPath("options")) {
      parseStringMap(config.getConfig("options"))
    } else Map.empty[String, String]
    
    SparkConfig(appName, master, options)
  }

  private def parseSparkKafkaConsumerConfig(config: Config): SparkKafkaConsumerConfig = {
    val options = if (config.hasPath("options")) {
      parseStringMap(config.getConfig("options"))
    } else Map.empty[String, String]
    
    SparkKafkaConsumerConfig(options)
  }

  private def parseHiveConfig(config: Config): HiveConfig = {
    val options = if (config.hasPath("options")) {
      parseStringMap(config.getConfig("options"))
    } else Map.empty[String, String]
    
    HiveConfig(options)
  }

  private def parseStringMap(config: Config): Map[String, String] = {
    config.entrySet().asScala.map { entry =>
    entry.getKey -> entry.getValue.unwrapped().toString
    }.toMap
  }
}
