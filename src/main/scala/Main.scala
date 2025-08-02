package bigdata.dwbi.mci

import core.configs.ConfigManager

object Main {
  def main(args: Array[String]): Unit = {
  val config = ConfigManager.loadConfig() 
  println(config)
  }

  private def runApplication(): Unit = {
  }
}
