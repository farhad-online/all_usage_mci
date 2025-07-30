package bigdata.dwbi.mci
package config.spark

import config.{ConfigValidation, CustomProperties}
import utils.logger.CustomLogger

import org.apache.spark.sql.SparkSession


case class SparkConfig(
                        appName: String,
                        master: String = "local[*]",
                        executorMemory: String = "2g",
                        driverMemory: String = "1g",
                        executorCores: Int = 2,
                        maxResultSize: String = "1g",
                        serializer: String = "org.apache.spark.serializer.KryoSerializer",
                        dynamicAllocation: Boolean = false,
                        checkpointLocation: Option[String] = None,
                        CustomConfigs: Map[String, String] = Map.empty,
                      ) extends ConfigValidation with CustomProperties[SparkConfig] with CustomLogger {

  def toSparkSession: SparkSession = {
    val builder = SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.executor.memory", executorMemory)
      .config("spark.driver.memory", driverMemory)
      .config("spark.executor.cores", executorCores.toString)
      .config("spark.driver.maxResultSize", maxResultSize)
      .config("spark.serializer", serializer)
      .config("spark.dynamicAllocation.enabled", dynamicAllocation.toString)

    checkpointLocation.foreach(location =>
      builder.config("spark.sql.streaming.checkpointLocation", location)
    )

    CustomConfigs.foreach { case (key, value) =>
      builder.config(key, value)
    }

    builder.getOrCreate()
  }

  override def validate(): Either[List[String], Unit] = {
    val validations = List(
      validateNonEmpty(appName, "appName"),
      validateNonEmpty(master, "master"),
      validatePositive(executorCores, "executorCores"),
      validateMemoryFormat(executorMemory, "executorMemory"),
      validateMemoryFormat(driverMemory, "driverMemory"),
      validateMemoryFormat(maxResultSize, "maxResultSize")
    )

    val errors = validations.collect { case Left(error) => error }
    if (errors.isEmpty) Right(()) else Left(errors)
  }

  private def validateMemoryFormat(memory: String, fieldName: String): Either[String, Unit] = {
    Right(())
    // val memoryPattern = "^\\d+[mgMG]$".r
    // if (memoryPattern.matches(memory)) Right(())
    // else Left(s"$fieldName must be in format like '1g' or '512m', got: $memory")
  }

  override def customProperties: Map[String, String] = {
    customProperties
  }
}