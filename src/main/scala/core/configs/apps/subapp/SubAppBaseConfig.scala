package bigdata.dwbi.mci
package core.configs.apps.subapp

import bigdata.dwbi.mci.core.configs.hive.SparkHiveConfig
import bigdata.dwbi.mci.core.configs.kafka.SparkKafkaConsumerConfig
import bigdata.dwbi.mci.core.configs.spark.SparkConfig

sealed trait SubAppBaseConfig {
  def name: String

  def env: String

  def enabled: Boolean

  def spark: SparkConfig

  def sparkKafkaConsumer: SparkKafkaConsumerConfig

  def sparkHive: SparkHiveConfig

  def options: Map[String, String] = Map.empty
}
