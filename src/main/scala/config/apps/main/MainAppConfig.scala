package bigdata.dwbi.mci
package config.apps.main

import config.apps.AppBaseConfig
import config.apps.subapp.SubAppConfig
import config.hive.HiveConfig
import config.kafka.{KafkaConsumerConfig, KafkaProducerConfig}
import config.spark.SparkConfig
import utils.logger.CustomLogger

case class MainAppConfig(
                          appName: String,
                          environment: String,
                          version: String,
                          spark: SparkConfig,
                          kafka: Option[KafkaConsumerConfig] = None,
                          kafkaProducer: Option[KafkaProducerConfig] = None,
                          hive: Option[HiveConfig] = None,
                          subApps: Map[String, SubAppConfig] = Map.empty
                        ) extends AppBaseConfig with CustomLogger {

  override def validate(): Either[List[String], Unit] = {
    val baseValidations = List(
      validateNonEmpty(appName, "appName"),
      validateNonEmpty(environment, "environment"),
      validateNonEmpty(version, "version")
    )

    val componentValidations = List(
      spark.validate().left.map(errors => s"Spark config errors: ${errors.mkString(", ")}"),
      kafka.map(_.validate().left.map(errors => s"Kafka config errors: ${errors.mkString(", ")}")).getOrElse(Right(())),
      kafkaProducer.map(_.validate().left.map(errors => s"Kafka Producer config errors: ${errors.mkString(", ")}")).getOrElse(Right(())),
      hive.map(_.validate().left.map(errors => s"Hive config errors: ${errors.mkString(", ")}")).getOrElse(Right(()))
    )

    val subAppValidations = subApps.map { case (name, config) =>
      config.validate().left.map(errors => s"SubApp '$name' errors: ${errors.mkString(", ")}")
    }.toList

    val allValidations = baseValidations ++ componentValidations ++ subAppValidations
    val errors = allValidations.collect { case Left(error) => error }

    if (errors.isEmpty) Right(()) else Left(errors)
  }
}

