package bigdata.dwbi.mci
package jobs.allusage.pgw_new.etl.transforms

import bigdata.dwbi.mci.core.logger.Logger
import org.apache.spark.sql.DataFrame

object TransformPgwNew extends Logger {
  def process(df: DataFrame): DataFrame = {
    // val formatted = df.map(x => Row(
    //   x(8).trim, // a_number
    //   "", // b_number
    //   str_int('0' + x(25).trim), //duration
    //   "p", //cdr_type
    //   x(13).trim, //imei
    //   x(10).trim, //imsi
    //   get_ms_location(x(14).trim, x(15).trim, x(16).trim), // ms location
    //   str_long('0' + x(27).trim), //usage
    //   x(39).trim, // rat_id
    //   x(23).substring(8, 8 + 6).trim, //start time
    //   str_int(DateConverter.toJalali(x(23).substring(0, 0 + 8).trim)) //start date
    // ))
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

  private def get_ms_location(mccmnc: String, lac: String, cell: String): String = {
    var zlac = "%05d".format(str_int(lac))
    var zcell = "%05d".format(str_int(cell))
    if (mccmnc == "43211") {
      var mslocation = mccmnc + zlac + zcell
      if (mslocation.length <= 15)
        return mslocation
      else
        return mccmnc + lac + zcell.substring(0, 0 + zcell.length - 3) + zcell.charAt(zcell.length - 1)
    }
    return "-1"
  }
}
