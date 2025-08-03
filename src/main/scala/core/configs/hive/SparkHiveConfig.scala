package bigdata.dwbi.mci
package core.configs.hive

import bigdata.dwbi.mci.core.configs.{ConfigValidation, CustomOptions}
import bigdata.dwbi.mci.core.logger.Logger


case class SparkHiveConfig(
                            tableName: String,
                            partitionKey: String,
                            options: Map[String, String] = Map.empty
                          ) extends ConfigValidation with CustomOptions[SparkHiveConfig] with Logger {

  def getConfig: Map[String, String] = {
    val hiveConfig = Map()
    hiveConfig.++(options)
  }

  override def validate(): Either[List[String], Unit] = {
    Right(())
  }
}
