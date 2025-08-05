package bigdata.dwbi.mci
package core.configs

import bigdata.dwbi.mci.core.logger.Logger
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

import java.io.InputStreamReader

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
        val fs: FileSystem = FileSystem.get(new Configuration())
        val fsInputStream: FSDataInputStream = fs.open(new Path(path))
        val reader = new InputStreamReader(fsInputStream)
        ConfigFactory.parseReader(reader)
          .withFallback(ConfigFactory.load())
          .withFallback(ConfigFactory.defaultApplication())
          .resolve()
      case None =>
        ConfigFactory.load()
          .withFallback(ConfigFactory.defaultApplication())
          .resolve()
    }
  }
}
