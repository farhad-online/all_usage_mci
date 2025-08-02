package bigdata.dwbi.mci
package core.configs.apps.subapp

import core.configs.hive.HiveConfig
import core.configs.kafka.SparkKafkaConsumerConfig
import core.configs.spark.SparkConfig
import core.configs.{ConfigValidation, CustomOptions}
import core.logger.Logger

case class SubAppBaseConfig(
                             name: String,
                             env: String,
                             enabled: Boolean = false,
                             spark: Option[SparkConfig] = None,
                             sparkKafkaConsumer: Option[SparkKafkaConsumerConfig] = None,
                             hive: Option[HiveConfig] = None,
                             options: Map[String, String] = Map.empty
                           ) extends ConfigValidation with CustomOptions[SubAppBaseConfig] with Logger {
  override def validate(): Either[List[String], Unit] = {
    Right(())
  }
}
