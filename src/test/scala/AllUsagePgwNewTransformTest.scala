import bigdata.dwbi.mci.jobs.allusage.cbs.etl.transforms.TransformCBS
import bigdata.dwbi.mci.jobs.allusage.pgw_new.etl.transforms.TransformPgwNew
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object AllUsagePgwNewTransformTest {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val sparkConf = new org.apache.spark.SparkConf()
      .setAppName("test_all_usage_cbs")

    val spark = SparkSession.builder
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // Mock Kafka data
    val sampleData = Seq(
      "9166|G_GECEFA05_EEFpgwcdr20250629181405000007701_74125778.dat|1|DATA|744668032||9|0001|989136078556||432113913255070|26.15.16.87||8688520700877812|43235|31027|164205012|90||||||20250629181221||103|X|0|0|0|||||189408773||nationalroam|||40|70||||9;108;;;;|EGEFD|43235||5.118.3.112|||||||||0|1|1|1|0|0|2|+0330|0|0||||G_GECEFA05_EEFpgwcdr20250629181405000007701_74125778.dat.data"
    )

    val inputSchema = StructType(Seq(
      StructField("value", StringType)
    ))

    val inputDF = spark.createDataFrame(
      sampleData.map(Tuple1(_)).toDF("value").rdd,
      inputSchema
    )

    val processedDF = TransformPgwNew.process(inputDF)
    processedDF.show()
  }
}