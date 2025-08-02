package bigdata.dwbi.mci
package core.configs


import core.logger.Logger


trait ConfigValidation extends Logger {
  def validate(): Either[List[String], Unit]

  protected def validateNonEmpty(value: String, fieldName: String): Either[String, Unit] = {
    if (value != null && value.trim.nonEmpty) Right(())
    else Left(s"$fieldName cannot be empty")
  }
}
