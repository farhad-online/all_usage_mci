package bigdata.dwbi.mci
package jobs.allusage.pgw_new.etl

import bigdata.dwbi.mci.core.connectors.hive.HiveSink
import bigdata.dwbi.mci.core.connectors.kafka.KafkaSource
import bigdata.dwbi.mci.core.connectors.console.ConsoleSink
import bigdata.dwbi.mci.core.logger.Logger
import bigdata.dwbi.mci.jobs.allusage.pgw_new.PgwNewConfig
import bigdata.dwbi.mci.jobs.allusage.pgw_new.etl.transforms.TransformPgwNew

object ETLPgqNew extends Logger {
  def run(): Unit = {
    val kafkaSource = new KafkaSource()
    val hiveSink = new HiveSink()
    // val consoleSink = new ConsoleSink()
    val spark = PgwNewConfig.spark.getSparkConfig
      .enableHiveSupport()
      .getOrCreate()

    val input = kafkaSource.getKafkaSource(spark, PgwNewConfig.sparkKafkaConsumer)
    val processedData = TransformPgwNew.process(input)
    //println(s"\n\n\n\n\n\n\n\n\n\n\n\n")
    //processedData.printSchema()
    //println(s"\n\n\n\n\n\n\n\n\n\n\n\n")
    //consoleSink.consoleSink(processedData)
    hiveSink.hiveSink(processedData, PgwNewConfig.sparkHive, PgwNewConfig.spark)
  }
}
