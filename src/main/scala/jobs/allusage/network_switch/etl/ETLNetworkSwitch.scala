package bigdata.dwbi.mci
package jobs.allusage.network_switch.etl

import bigdata.dwbi.mci.core.connectors.hive.HiveSink
import bigdata.dwbi.mci.core.connectors.kafka.KafkaSource
import bigdata.dwbi.mci.core.logger.Logger
import bigdata.dwbi.mci.jobs.allusage.network_switch.NetworkSwitchConfig
import bigdata.dwbi.mci.jobs.allusage.network_switch.etl.transformers.TransformNetworkSwitch

object ETLNetworkSwitch extends Logger {
  def run(): Unit = {
    val kafkaSource = new KafkaSource()
    val hiveSink = new HiveSink()
    val spark = NetworkSwitchConfig.spark.getSparkConfig
      .enableHiveSupport()
      .getOrCreate()

    val input = kafkaSource.getKafkaSource(spark, NetworkSwitchConfig.sparkKafkaConsumer)
    val processedData = TransformNetworkSwitch.process(input)
    hiveSink.hiveSink(processedData, NetworkSwitchConfig.sparkHive, NetworkSwitchConfig.spark)
  }

}
