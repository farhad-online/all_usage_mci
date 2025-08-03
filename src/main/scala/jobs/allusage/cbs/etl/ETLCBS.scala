package bigdata.dwbi.mci
package jobs.allusage.cbs.etl

import bigdata.dwbi.mci.core.connectors.hive.HiveSink
import bigdata.dwbi.mci.core.connectors.kafka.KafkaSource
import bigdata.dwbi.mci.core.logger.Logger
import bigdata.dwbi.mci.jobs.allusage.cbs.etl.transforms.TransformCBS
import bigdata.dwbi.mci.jobs.allusage.network_switch.etl.transformers.TransformNetworkSwitch
import jobs.allusage.cbs.CBSConfig.CBSConfig

object ETLCBS extends Logger {
  def run(cbsConfig: CBSConfig): Unit = {
    val kafkaSource = new KafkaSource()
    val hiveSink = new HiveSink()
    val spark = cbsConfig.spark.getSparkConfig
      .enableHiveSupport()
      .getOrCreate()

    val input = kafkaSource.getKafkaSource(spark, cbsConfig.sparkKafkaConsumer)
    val processedData = TransformCBS.process(input)
    hiveSink.hiveSink(processedData, cbsConfig.sparkHive, cbsConfig.spark)
  }

}
