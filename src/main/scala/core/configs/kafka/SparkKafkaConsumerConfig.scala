package bigdata.dwbi.mci
package core.configs.kafka

import bigdata.dwbi.mci.core.configs.{ConfigValidation, CustomOptions}
import bigdata.dwbi.mci.core.logger.Logger

case class SparkKafkaConsumerConfig(
                                     format: String,
                                     options: Map[String, String] = Map.empty
                                   ) extends ConfigValidation with CustomOptions[SparkKafkaConsumerConfig] with Logger {

  def getConfig: Map[String, String] = {
    val sparkKafkaConsumerConfig = Map()
    sparkKafkaConsumerConfig.++(options)
  }

  override def validate(): Either[List[String], Unit] = {
    Right(())
  }
}
