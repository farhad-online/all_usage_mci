package bigdata.dwbi.mci
package core.connectors.kafka

import bigdata.dwbi.mci.core.configs.kafka.SparkKafkaConsumerConfig
import core.logger.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaSource extends Logger{
  def getKafkaSource(spark: SparkSession, sparkKafkaConsumerConfig: SparkKafkaConsumerConfig): DataFrame = {
    spark.readStream
      .format(sparkKafkaConsumerConfig.format)
      .options(sparkKafkaConsumerConfig.options)
      .load()
      .selectExpr("CAST(value AS STRING) as value")
  }

}
