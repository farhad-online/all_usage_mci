package bigdata.dwbi.mci
package core.configs.hive

import core.configs.{ConfigValidation, CustomOptions}
import core.logger.Logger


case class HiveConfig(
                       options: Map[String, String] = Map.empty
                     ) extends ConfigValidation with CustomOptions[HiveConfig] with Logger {

  def getConfig: Map[String, String] = {
    val hiveConfig = Map()
    hiveConfig.++(options)
  }

  override def validate(): Either[List[String], Unit] = {
    Right(())
  }
}
