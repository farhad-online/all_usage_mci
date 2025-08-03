package bigdata.dwbi.mci
package utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object SparkUDF {
  val toJalaliUDF: UserDefinedFunction = udf((date: String) => {
    if (date != null && date.nonEmpty) {
      try {
        Some(DateConverter.toJalali(date).toInt)
      } catch {
        case _: NumberFormatException => None
      }
    } else {
      None
    }
  })

}
