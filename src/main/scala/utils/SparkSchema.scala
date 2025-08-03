package bigdata.dwbi.mci
package utils

import org.apache.spark.sql.types._

object SparkSchema {
  val allUsageSchema: StructType = new StructType()
    .add(StructField("a_number", StringType, true))
    .add(StructField("b_number", StringType, true))
    .add(StructField("duration", IntegerType, true))
    .add(StructField("cdr_type", StringType, true))
    .add(StructField("imei", StringType, true))
    .add(StructField("imsi", StringType, true))
    .add(StructField("ms_location", StringType, true))
    .add(StructField("usage", LongType, true))
    .add(StructField("rat_id", StringType, true))
    .add(StructField("start_time", StringType, true))
    .add(StructField("start_date", IntegerType, true))
}