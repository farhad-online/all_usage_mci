package bigdata.dwbi.mci
package jobs.allusage.network_switch

import bigdata.dwbi.mci.core.configs.ConfigManager
import bigdata.dwbi.mci.core.configs.ConfigModule.ConfigModule
import bigdata.dwbi.mci.core.configs.hive.SparkHiveConfig
import bigdata.dwbi.mci.core.configs.kafka.SparkKafkaConsumerConfig
import bigdata.dwbi.mci.core.configs.spark.SparkConfig
import com.typesafe.config.Config

object NetworkSwitchConfig extends ConfigModule {

  lazy val name: String = config getString "all_usage.network_switch.name"
  lazy val env: String = config getString "all_usage.network_switch.env"
  lazy val enable: Boolean = config getBoolean "all_usage.network_switch.enable"
  lazy val spark: SparkConfig =

 // case class NetworkSwitchConfig(
 //                                 name: String,
 //                                 env: String,
 //                                 enabled: Boolean,
 //                                 spark: SparkConfig,
 //                                 sparkKafkaConsumer: SparkKafkaConsumerConfig,
 //                                 sparkHive: SparkHiveConfig,
 //                                 options: Map[String, String] = Map.empty
 //                               )

 // def loadConfig(config: Config): NetworkSwitchConfig = {
 //   val name = config.getString("name")
 //   val env = config.getString("env")
 //   val enabled = config.getBoolean("enabled")
 //   val spark = ConfigManager.parseSparkConfig(config.getConfig("spark"))
 //   val sparkKafkaConsumer = ConfigManager.parseSparkKafkaConsumerConfig(config.getConfig("sparkKafkaConsumer"))
 //   val sparkHive = ConfigManager.parseSparkHiveConfig(config.getConfig("sparkHive"))
 //   val options = ConfigManager.parseStringMap(config.getConfig("options"))

 //   NetworkSwitchConfig(name, env, enabled, spark, sparkKafkaConsumer, sparkHive, options)
 // }
}
