package bigdata.dwbi.mci
package jobs.allusage.network_switch.etl

import core.connectors.hive.HiveSink
import core.connectors.kafka.KafkaSource
import core.logger.Logger
import jobs.allusage.network_switch.NetworkSwitchConfig
import jobs.allusage.network_switch.etl.transformers.TransformNetworkSwitch

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
