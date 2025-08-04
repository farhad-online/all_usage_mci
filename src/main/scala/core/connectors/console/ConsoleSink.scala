package bigdata.dwbi.mci
package core.connectors.console


import bigdata.dwbi.mci.core.logger.Logger
import org.apache.spark.sql.DataFrame


class ConsoleSink extends Logger {
  def consoleSink(df: DataFrame): Unit = {
    df
      .writeStream
      .outputMode("append")
      .option("truncate", false)
      .format("console")
      .start()
      .awaitTermination()
  }
}
