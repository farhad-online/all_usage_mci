package bigdata.dwbi.mci
package utils.logger

import org.apache.logging.log4j.{LogManager, Logger}

trait CustomLogger {
  protected val logger: Logger = LogManager.getLogger(getClass)
}