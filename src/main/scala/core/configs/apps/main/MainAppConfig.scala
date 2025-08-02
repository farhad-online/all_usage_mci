package bigdata.dwbi.mci
package core.configs.apps.main

import core.configs.apps.subapp.SubAppBaseConfig
import core.logger.Logger
import core.configs.ConfigValidation


case class MainAppConfig(
                          appName: String,
                          environment: String,
                          version: String,
                          subApps: Map[String, SubAppBaseConfig] = Map.empty
                        ) extends ConfigValidation with Logger {

  override def validate(): Either[List[String], Unit] = {
    Right(())
  }
}
