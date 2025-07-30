package bigdata.dwbi.mci
package config

import bigdata.dwbi.mci.utils.logger.CustomLogger

import scala.util.{Failure, Success, Try}


trait ConfigValidation extends CustomLogger{
  def validate(): Either[List[String], Unit]

  protected def validateNonEmpty(value: String, fieldName: String): Either[String, Unit] = {
    if (value != null && value.trim.nonEmpty) Right(())
    else Left(s"$fieldName cannot be empty")
  }

  protected def validatePositive(value: Int, fieldName: String): Either[String, Unit] = {
    if (value > 0) Right(())
    else Left(s"$fieldName must be positive, got: $value")
  }

  protected def validateRange(value: Int, min: Int, max: Int, fieldName: String): Either[String, Unit] = {
    if (value >= min && value <= max) Right(())
    else Left(s"$fieldName must be between $min and $max, got: $value")
  }

  protected def validateUrl(url: String, fieldName: String): Either[String, Unit] = {
    Try(new java.net.URL(url)) match {
      case Success(_) => Right(())
      case Failure(_) => Left(s"$fieldName is not a valid URL: $url")
    }
  }
}
