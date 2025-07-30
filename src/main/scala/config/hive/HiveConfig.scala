package bigdata.dwbi.mci
package config.hive

import config.{ConfigValidation, CustomProperties}
import utils.logger.CustomLogger

case class HiveConfig(
                       metastoreUri: String,
                       warehouse: String,
                       database: String = "default",
                       enableStats: Boolean = true,
                       enableCBO: Boolean = true,
                       dynamicPartitioning: Boolean = true,
                       maxDynamicPartitions: Int = 1000,
                       compressionCodec: String = "snappy",
                       customProperties: Map[String, String] = Map.empty
                     ) extends ConfigValidation with CustomProperties[HiveConfig] with CustomLogger {

  def toHiveConfigs: Map[String, String] = {
    val hiveConfig = Map(
      "hive.metastore.uris" -> metastoreUri,
      "hive.metastore.warehouse.dir" -> warehouse,
      "hive.stats.autogather" -> enableStats.toString,
      "hive.cbo.enable" -> enableCBO.toString,
      "hive.exec.dynamic.partition" -> dynamicPartitioning.toString,
      "hive.exec.max.dynamic.partitions" -> maxDynamicPartitions.toString,
      "hive.exec.compress.output" -> "true",
      "hive.exec.compress.intermediate" -> "true",
      "mapred.output.compression.codec" -> s"org.apache.hadoop.io.compress.${compressionCodec.capitalize}Codec",
    )
    hiveConfig.++(customProperties)
  }

  override def validate(): Either[List[String], Unit] = {
    val validations = List(
      validateNonEmpty(metastoreUri, "metastoreUri"),
      validateNonEmpty(warehouse, "warehouse"),
      validateNonEmpty(database, "database"),
      validatePositive(maxDynamicPartitions, "maxDynamicPartitions"),
      validateCompressionCodec(compressionCodec)
    )

    val errors = validations.collect { case Left(error) => error }
    if (errors.isEmpty) Right(()) else Left(errors)
  }

  private def validateCompressionCodec(codec: String): Either[String, Unit] = {
    val supportedCodecs = Set("snappy", "gzip", "lzo", "bzip2", "zstd", "zstandard")
    if (supportedCodecs.contains(codec.toLowerCase)) Right(())
    else Left(s"Unsupported compression codec: $codec. Supported: ${supportedCodecs.mkString(", ")}")
  }
}