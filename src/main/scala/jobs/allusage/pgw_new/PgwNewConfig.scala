package bigdata.dwbi.mci
package jobs.allusage.pgw_new

import bigdata.dwbi.mci.core.configs.ConfigManager
import bigdata.dwbi.mci.core.configs.hive.SparkHiveConfig
import bigdata.dwbi.mci.core.configs.kafka.SparkKafkaConsumerConfig
import bigdata.dwbi.mci.core.configs.spark.SparkConfig
import bigdata.dwbi.mci.jobs.allusage.network_switch.NetworkSwitchConfig.config

object PgwNewConfig {
  private val configPrefix: String = "all_usage.pgw_new"

  lazy val name: String = config getString s"${configPrefix}.name"
  lazy val env: String = config getString s"${configPrefix}.env"
  lazy val enable: Boolean = config getBoolean s"${configPrefix}.enable"
  lazy val spark: SparkConfig = ConfigManager.parseSparkConfig(config.getConfig(s"${configPrefix}.spark"))
  lazy val sparkKafkaConsumer: SparkKafkaConsumerConfig = ConfigManager.parseSparkKafkaConsumerConfig(config.getConfig(s"${configPrefix}.sparkKafkaConsumer"))
  lazy val sparkHive: SparkHiveConfig = ConfigManager.parseSparkHiveConfig(config.getConfig(s"${configPrefix}.sparkHive"))
}
