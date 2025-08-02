package bigdata.dwbi.mci
package core.configs

import core.logger.Logger

trait CustomOptions[T] extends Logger{
  def options: Map[String, String]

  def getOption(key: String): Option[Any] = options.get(key)

  def hasOption(key: String): Boolean = options.contains(key)

  protected def validateCustomOptionBase(): Either[String, Unit] = {
    val invalidProperties = options.filter { case (key, value) =>
      key == null || key.trim.isEmpty || value == null
    }

    if (invalidProperties.isEmpty) Right(())
    else Left(s"Invalid custom properties found: ${invalidProperties.keys.mkString(", ")} - keys cannot be null/empty and values cannot be null")
  }
}
