package bigdata.dwbi.mci
package jobs.allusage.pgw_new.etl

import bigdata.dwbi.mci.core.connectors.hive.HiveSink
import bigdata.dwbi.mci.core.connectors.kafka.KafkaSource
import bigdata.dwbi.mci.core.logger.Logger
import bigdata.dwbi.mci.jobs.allusage.pgw_new.PgwNewConfig.PgwNewConfig
import bigdata.dwbi.mci.jobs.allusage.pgw_new.etl.transforms.TransformPgwNew

object ETLPgqNew extends Logger {
  def run(pgwNewConfig: PgwNewConfig): Unit = {
    val kafkaSource = new KafkaSource()
    val hiveSink = new HiveSink()
    val spark = pgwNewConfig.spark.getSparkConfig
      .enableHiveSupport()
      .getOrCreate()

    val input = kafkaSource.getKafkaSource(spark, pgwNewConfig.sparkKafkaConsumer)
    val processedData = TransformPgwNew.process(input)
    hiveSink.hiveSink(processedData, pgwNewConfig.sparkHive, pgwNewConfig.spark)
  }
}
