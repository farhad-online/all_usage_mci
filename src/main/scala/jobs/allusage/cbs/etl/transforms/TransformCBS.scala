package bigdata.dwbi.mci
package jobs.allusage.cbs.etl.transforms

import bigdata.dwbi.mci.utils.DateUtil
import org.apache.spark.sql.DataFrame

object TransformCBS {
  def process(df: DataFrame): DataFrame = {
    df
  }

  private def str_int(value: String): Int = {
    try {
      if (value == "") 0 else if (value(0) == '-') value.toInt else ('0' + value).toInt
    }
    catch {
      case _: Throwable => 0
    }

  }

  private def str_long(value: String): Long = {
    try {
      if (value == "") 0L else if (value(0) == '-') value.toLong else ('0' + value).toLong
    }
    catch {
      case _: Throwable => 0
    }
  }

  private def get_start_time(value: String): String = {
    try {
      val start_time = value.split('|')(14).substring(8, 8 + 6)
      start_time
    }
    catch {
      case _: Throwable => "0"
    }
  }

  private def get_start_date(value: String): Int = {
    try {
      val start_date = str_int(value.split('|')(14).substring(0, 8))
      start_date
    }
    catch {
      case _: Throwable => 0
    }
  }

  private def get_a_number(value: String): String = {
    try {
      val a_number = value.split('|')(25).trim
      a_number
    }
    catch {
      case _: Throwable => "0"
    }
  }

  private def get_duration(value: String): Long = {
    try {

      val duration = DateUtil.get_dif_time2(value.split('|')(14), value.split('|')(15))
      duration
    }

    catch {
      case _: Throwable => 0L
    }

  }

  private def get_imei(value: String): String = {
    try {
      val imei = value.split('|')(505).trim
      imei
    }

    catch {
      case _: Throwable => "0"
    }
  }

  private def get_imsi(value: String): String = {
    try {
      val imsi = value.split('|')(494).trim
      imsi
    }

    catch {
      case _: Throwable => "0"
    }
  }

  private def get_ms_location(value: String): String = {
    try {
      val c = value.split('|')
      if (c(498).trim.length >= 5) {
        if (c(498).substring(0, 5) == "43211") {
          if (c(498).length == 15)
            return c(498)
          else
            return "43211" +
              Integer.parseInt(c(498).substring(5, 5 + 4), 16).toString +
              (Integer.parseInt(c(498).substring(9), 16) / 256).toString +
              (Integer.parseInt(c(498).substring(9), 16) % 256).toString
        }
        else c(498).substring(0, 5)
      }
      "-1"

    }

    catch {
      case _: Throwable => "0"
    }
  }
}
