package bigdata.dwbi.mci
package core.configs

import core.logger.Logger

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File


object ConfigModule extends Logger {
  trait ConfigModule {
    lazy val config: Config = ConfigModule.config
  }

  private var configFilePath: Option[String] = None

  def setConfigPath(path: String): Unit = {
    logger.debug(s"set new config path: ${path}")
    configFilePath = Some(path)
  }

  private lazy val config: Config = {
    configFilePath match {
      case Some(path) =>
        val configFile = new File(path)
        if (configFile.exists()) {
          ConfigFactory.parseFile(configFile)
            .withFallback(ConfigFactory.load())
            .withFallback(ConfigFactory.defaultApplication())
            .resolve()
        } else {
          logger.error(s"Config file not fount: $path")
          throw new RuntimeException(s"Config file not found: $path")
        }
      case None =>
        ConfigFactory.load()
          .withFallback(ConfigFactory.defaultApplication())
          .resolve()
    }
  }
}
