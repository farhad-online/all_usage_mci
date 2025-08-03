package bigdata.dwbi.mci

import bigdata.dwbi.mci.core.logger.Logger
import bigdata.dwbi.mci.jobs.allusage.cbs.etl.ETLCBS
import bigdata.dwbi.mci.jobs.allusage.network_switch.etl.ETLNetworkSwitch

object Main extends Logger {
  def main(args: Array[String]): Unit = {
    val jobName = args(0)
    jobName match {
      case "all_usage_network_switch" =>
        ETLNetworkSwitch.run()
      case "all_usage_pgw_new" =>
        ETLNetworkSwitch.run()
      case "all_usage_cbs" =>
        ETLCBS.run()
      case _ =>
        logger.error(s"Unknown job: $jobName")
        System.exit(1)
    }
  }
}
