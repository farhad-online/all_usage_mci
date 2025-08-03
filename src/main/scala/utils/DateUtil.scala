package bigdata.dwbi.mci
package utils

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

object DateUtil {
  def get_dif_time2(beg: String, end: String): Long = {
    try {
      val f = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
      val t1 = LocalDateTime.parse(DateConverter.toGeo(beg) + beg.substring(8), f)
      val t2 = LocalDateTime.parse(DateConverter.toGeo(end) + end.substring(8), f)
      ChronoUnit.SECONDS.between(t1, t2).toLong
    }
    catch {
      case _: Throwable => 0L
    }
  }
}
