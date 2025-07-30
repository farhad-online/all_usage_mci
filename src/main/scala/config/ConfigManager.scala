package bigdata.dwbi.mci
package config

import config.apps.main.MainAppConfig
import utils.logger.CustomLogger

import com.typesafe.config.ConfigFactory
import pureconfig._

import scala.util.{Failure, Success, Try}

object ConfigManager extends CustomLogger {

  @volatile
  private var _instance: Option[MainAppConfig] = None

  private val lock = new Object()

  def initialize(configPath: String = "application.conf"): Either[ConfigError, MainAppConfig] = {
    lock.synchronized {
      if (_instance.isEmpty) {
        loadConfig(configPath) match {
          case Right(config) =>
            config.validate() match {
              case Right(_) =>
                _instance = Some(config)
                Right(config)
              case Left(validationErrors) =>
                Left(ValidationError(s"Configuration validation failed: ${validationErrors.mkString(", ")}"))
            }
          case Left(error) => Left(error)
        }
      } else {
        Right(_instance.get)
      }
    }
  }

  def instance: MainAppConfig = {
    _instance.getOrElse(
      throw new IllegalStateException("ConfigManager not initialized. Call initialize() first.")
    )
  }

  def isInitialized: Boolean = _instance.isDefined

  def reload(configPath: String = "application.conf"): Either[ConfigError, MainAppConfig] = {
    lock.synchronized {
      _instance = None
      initialize(configPath)
    }
  }

  private def loadConfig(configPath: String): Either[ConfigError, MainAppConfig] = {
    Try {
      val config = ConfigFactory.load(configPath)
      ConfigSource.fromConfig(config).load[MainAppConfig]
    } match {
      case Success(configResult) => configResult.left.map(failures =>
        LoadError(s"Failed to load configuration: ${failures.prettyPrint()}")
      )
      case Failure(exception) => Left(LoadError(s"Failed to load config file: ${exception.getMessage}"))
    }
  }

  def sparkConfig = instance.spark

  def kafkaConfig = instance.kafka

  def kafkaProducerConfig = instance.kafkaProducer

  def hiveConfig = instance.hive

  def subAppConfig(name: String) = instance.subApps.get(name)

  def getConfigByEnvironment(env: String): Either[ConfigError, MainAppConfig] = {
    initialize(s"application-$env.conf")
  }

  def mergeWithSystemProperties(): MainAppConfig = {
    instance
  }
}

sealed trait ConfigError

case class LoadError(message: String) extends ConfigError

case class ValidationError(message: String) extends ConfigError
