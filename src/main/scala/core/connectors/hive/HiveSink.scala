package bigdata.dwbi.mci
package core.connectors.hive

import core.configs.hive.SparkHiveConfig
import core.configs.spark.SparkConfig
import core.logger.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

class HiveSink extends Logger {
  def hiveSink(df: DataFrame, hiveConfig: SparkHiveConfig, sparkConfig: SparkConfig): Unit = {
    val query = df.writeStream
      .outputMode(sparkConfig.outputMode)
      .format(sparkConfig.format)
      .option("checkpointLocation", sparkConfig.checkpointLocation)
      .trigger(Trigger.ProcessingTime(s"${sparkConfig.triggerInterval} minutes"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          batchDF.write
            .format(sparkConfig.batchFormat)
            .mode(sparkConfig.batchMode)
            .partitionBy(hiveConfig.partitionKey)
            .saveAsTable(hiveConfig.tableName)
        }
      }.start()

    query.awaitTermination()
  }
}
