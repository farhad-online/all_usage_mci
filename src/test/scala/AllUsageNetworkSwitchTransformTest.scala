import bigdata.dwbi.mci.jobs.allusage.cbs.etl.transforms.TransformCBS
import bigdata.dwbi.mci.jobs.allusage.network_switch.etl.transformers.TransformNetworkSwitch
import bigdata.dwbi.mci.jobs.allusage.pgw_new.etl.transforms.TransformPgwNew
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object AllUsageNetworkSwitchTransformTest {
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
      "00000052241432112975206246989184582011    989337199143   11  20250701174434000030432113087130202   000030D000000000    0160000000000FA663A663B00000000989180420      351890419810280 H10      H_MSSJA0_HU0038_20250701180025.r006.encoded"
    )

    val inputSchema = StructType(Seq(
      StructField("value", StringType)
    ))

    val inputDF = spark.createDataFrame(
      sampleData.map(Tuple1(_)).toDF("value").rdd,
      inputSchema
    )

    val processedDF = TransformNetworkSwitch.process(inputDF)
    processedDF.show()
  }
}