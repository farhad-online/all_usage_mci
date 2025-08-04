package bigdata.dwbi.mci
package jobs.allusage.cbs.etl.transforms

import utils.SparkUDF.{getMsLocationCBSUDF, strIntUDF, strLongUDF}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TransformCBS {
  def process(df: DataFrame): DataFrame = {
    df
      .withColumn("split_value", split(col("value"), "|"))
      .filter(length(col("split_value")).>=(506))
      .select(
        coalesce(trim(col("split_value")(25)), lit("0")).as("a_number"),
        lit("").as("b_number"),
        concat(col("split_value")(14), col("split_value")(15)).as("duration"),
        lit("c").as("cdr_type"),
        trim(col("split_value")(505)).as("imei"),
        trim(col("split_value")(494)).as("imsi"),
        getMsLocationCBSUDF(col("split_value")(498)).as("ms_location"),
        strLongUDF(concat(lit("0"), col("split_value")(38))).as("usage"),
        lit("").as("rat_id"),
        substring(col("value"), 9, 6).as("start_time"),
        strIntUDF(substring(col("value"), 1, 8)).as("start_date")
      )
  }
}
