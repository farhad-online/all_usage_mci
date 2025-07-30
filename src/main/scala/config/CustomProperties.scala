package bigdata.dwbi.mci
package config

import utils.logger.CustomLogger

trait CustomProperties[T] extends CustomLogger{
  def customProperties: Map[String, String]

  def getCustomProperty(key: String): Option[String] = customProperties.get(key)

  def hasCustomProperty(key: String): Boolean = customProperties.contains(key)

  protected def validateCustomPropertiesBase(): Either[String, Unit] = {
    val invalidProperties = customProperties.filter { case (key, value) =>
      key == null || key.trim.isEmpty || value == null
    }

    if (invalidProperties.isEmpty) Right(())
    else Left(s"Invalid custom properties found: ${invalidProperties.keys.mkString(", ")} - keys cannot be null/empty and values cannot be null")
  }
}
