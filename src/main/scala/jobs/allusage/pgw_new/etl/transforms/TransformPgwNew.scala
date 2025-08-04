package bigdata.dwbi.mci
package jobs.allusage.pgw_new.etl.transforms

import bigdata.dwbi.mci.core.logger.Logger
import bigdata.dwbi.mci.utils.SparkUDF.{getMsLocationPGWUDF, strIntUDF, strLongUDF, toJalaliUDF}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TransformPgwNew extends Logger {
  def process(df: DataFrame): DataFrame = {
    df
      .withColumn("split_value", split(col("value"), "|"))
      .filter(size(col("split_value")).===(71))
      .select(
        trim(col("split_value")(8)).as("a_number"),
        lit("").as("b_number"),
        strIntUDF(concat(lit("0"), trim(col("split_value")(25)))).as("duration"),
        lit("p").as("cdr_type"),
        trim(col("split_value")(13)).as("imei"),
        trim(col("split_value")(10)).as("imsi"),
        getMsLocationPGWUDF(trim(col("split_value")(14)), trim(col("split_value")(15)), trim(col("split_value")(16))).as("ms_location"),
        strLongUDF(concat(lit("0"), trim(col("split_value")(27)))).as("usage"),
        trim(col("split_value")(39)).as("rat_id"),
        trim(substring(col("split_value")(23), 9, 6)).as("start_time"),
        strIntUDF(toJalaliUDF(trim(substring(col("split_value")(23), 1, 8)))).as("start_date")
      )
  }
}
