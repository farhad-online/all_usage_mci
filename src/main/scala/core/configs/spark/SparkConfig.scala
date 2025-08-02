package bigdata.dwbi.mci
package core.configs.spark

import core.configs.{ConfigValidation, CustomOptions}
import core.logger.Logger

import org.apache.spark.sql.SparkSession



case class SparkConfig(
                        appName: String,
                        master: String,
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
