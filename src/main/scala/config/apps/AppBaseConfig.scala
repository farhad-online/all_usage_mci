package bigdata.dwbi.mci
package config.apps

import config.ConfigValidation
import utils.logger.CustomLogger

trait AppBaseConfig extends ConfigValidation with CustomLogger {
  def appName: String

  def environment: String
}
