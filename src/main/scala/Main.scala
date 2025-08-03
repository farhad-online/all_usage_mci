package bigdata.dwbi.mci

import core.configs.ConfigModule
import core.logger.Logger
import jobs.allusage.cbs.etl.ETLCBS
import jobs.allusage.network_switch.etl.ETLNetworkSwitch

object Main extends Logger {
  def main(args: Array[String]): Unit = {
    logger.info("Starting running Job")
    if (args.length > 0) {
      ConfigModule.setConfigPath(args(1))
    }

    val jobName = args(0)
    logger.debug(s"get argument: jobName: ${jobName}")
    jobName match {
      case "all_usage_network_switch" =>
        logger.debug("prerun all_usage_network_switch")
        ETLNetworkSwitch.run()
      case "all_usage_pgw_new" =>
        logger.debug("prerun all_usage_pgw_new")
        ETLNetworkSwitch.run()
      case "all_usage_cbs" =>
        logger.debug("prerun all_usage_cbs")
        ETLCBS.run()
      case _ =>
        logger.error(s"Invalid job name: $jobName")
        System.exit(1)
    }
  }
}
