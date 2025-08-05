package bigdata.dwbi.mci
package jobs.allusage.cbs.etl

import bigdata.dwbi.mci.core.connectors.console.ConsoleSink
import bigdata.dwbi.mci.core.connectors.hive.HiveSink
import bigdata.dwbi.mci.core.connectors.kafka.KafkaSource
import bigdata.dwbi.mci.core.logger.Logger
import bigdata.dwbi.mci.jobs.allusage.cbs.CBSConfig
import bigdata.dwbi.mci.jobs.allusage.cbs.etl.transforms.TransformCBS
import bigdata.dwbi.mci.utils.SparkSchema

object ETLCBS extends Logger {
  def run(): Unit = {
    val kafkaSource = new KafkaSource()
    val hiveSink = new HiveSink()
    val spark = CBSConfig.spark.getSparkConfig
      .enableHiveSupport()
      .getOrCreate()

    val input = kafkaSource.getKafkaSource(spark, CBSConfig.sparkKafkaConsumer)
    val processedData = TransformCBS.process(input)
    hiveSink.hiveSink(processedData, CBSConfig.sparkHive, CBSConfig.spark)
  }

}
