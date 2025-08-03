package bigdata.dwbi.mci
package core.configs

import com.typesafe.config.{Config, ConfigFactory}


object ConfigModule {
  trait ConfigModule {
    lazy val config: Config = ConfigModule.config
  }

  private lazy val config: Config = ConfigFactory.load().withFallback(ConfigFactory.defaultApplication()).resolve

}
