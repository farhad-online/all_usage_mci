package bigdata.dwbi.mci
package core.configs.spark

import bigdata.dwbi.mci.core.configs.{ConfigValidation, CustomOptions}
import bigdata.dwbi.mci.core.logger.Logger
import org.apache.spark.sql.SparkSession


case class SparkConfig(
                        appName: String,
                        master: String,
                        checkpointLocation: String,
                        outputMode: String,
                        batchMode: String,
                        batchFormat: String,
                        format: String,
                        triggerInterval: Int,
                        options: Map[String, String] = Map.empty,
                      ) extends ConfigValidation with CustomOptions[SparkConfig] with Logger {

  def getSparkConfig: SparkSession.Builder = {
    val builder = SparkSession.builder()
      .appName(appName)
      .master(master)

    options.foreach { case (key, value) =>
      builder.config(key, value)
    }

    builder
  }

  override def validate(): Either[List[String], Unit] = {
    Right(())
  }
}
