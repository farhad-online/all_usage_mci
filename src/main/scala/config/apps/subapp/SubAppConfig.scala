package bigdata.dwbi.mci
package config.apps.subapp

import config.apps.AppBaseConfig
import config.hive.HiveConfig
import config.kafka.{KafkaConsumerConfig, KafkaProducerConfig}
import config.spark.SparkConfig
import utils.logger.CustomLogger

case class SubAppConfig(
                         name: String,
                         enabled: Boolean = true,
                         spark: Option[SparkConfig] = None,
                         kafka: Option[KafkaConsumerConfig] = None,
                         kafkaProducer: Option[KafkaProducerConfig] = None,
                         hive: Option[HiveConfig] = None,
                         customConfigs: Map[String, String] = Map.empty
                       ) extends AppBaseConfig with CustomLogger {

  override def appName: String = name

  override def environment: String = "sub-app"

  override def validate(): Either[List[String], Unit] = {
    val validations = List(
      validateNonEmpty(name, "name"),
      spark.map(_.validate().left.map(errors => s"Spark errors: ${errors.mkString(", ")}")).getOrElse(Right(())),
      kafka.map(_.validate().left.map(errors => s"Kafka errors: ${errors.mkString(", ")}")).getOrElse(Right(())),
      kafkaProducer.map(_.validate().left.map(errors => s"Kafka Producer errors: ${errors.mkString(", ")}")).getOrElse(Right(())),
      hive.map(_.validate().left.map(errors => s"Hive errors: ${errors.mkString(", ")}")).getOrElse(Right(()))
    )

    val errors = validations.collect { case Left(error) => error }
    if (errors.isEmpty) Right(()) else Left(errors)
  }
}
