package bigdata.dwbi.mci
package jobs.allusage.cbs

import bigdata.dwbi.mci.core.configs.ConfigManager
import bigdata.dwbi.mci.core.configs.ConfigModule.ConfigModule
import bigdata.dwbi.mci.core.configs.hive.SparkHiveConfig
import bigdata.dwbi.mci.core.configs.kafka.SparkKafkaConsumerConfig
import bigdata.dwbi.mci.core.configs.spark.SparkConfig
import com.typesafe.config.Config

object CBSConfig extends ConfigModule {
  case class CBSConfig(
                        name: String,
                        env: String,
                        enabled: Boolean,
                        spark: SparkConfig,
                        sparkKafkaConsumer: SparkKafkaConsumerConfig,
                        sparkHive: SparkHiveConfig,
                        options: Map[String, String] = Map.empty
                      )

  def loadConfig(config: Config): CBSConfig = {
    val name = config.getString("name")
    val env = config.getString("env")
    val enabled = config.getBoolean("enabled")
    val spark = ConfigManager.parseSparkConfig(config.getConfig("spark"))
    val sparkKafkaConsumer = ConfigManager.parseSparkKafkaConsumerConfig(config.getConfig("sparkKafkaConsumer"))
    val sparkHive = ConfigManager.parseSparkHiveConfig(config.getConfig("sparkHive"))
    val options = ConfigManager.parseStringMap(config.getConfig("options"))

    CBSConfig(name, env, enabled, spark, sparkKafkaConsumer, sparkHive, options)
  }
}
