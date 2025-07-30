package bigdata.dwbi.mci
package config.kafka

import config.{ConfigValidation, CustomProperties}
import utils.logger.CustomLogger

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties

case class KafkaProducerConfig(
                                bootstrapServers: String,
                                acks: String = "all",
                                retries: Int = 3,
                                batchSize: Int = 16384,
                                lingerMs: Int = 1,
                                bufferMemory: Long = 33554432L,
                                keySerializer: String = classOf[StringDeserializer].getName,
                                valueSerializer: String = classOf[StringDeserializer].getName,
                                customProperties: Map[String, String] = Map.empty
                              ) extends ConfigValidation with CustomProperties[KafkaProducerConfig] with CustomLogger {

  def toKafkaProperties: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.ACKS_CONFIG, acks)
    props.put(ProducerConfig.RETRIES_CONFIG, retries.toString)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize.toString)
    props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs.toString)
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory.toString)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer)
    customProperties.foreach { case (key, value) =>
      props.put(key, value)
    }
    props
  }

  override def validate(): Either[List[String], Unit] = {
    val validations = List(
      validateNonEmpty(bootstrapServers, "bootstrapServers"),
      validateRange(retries, 0, 10, "retries"),
      validatePositive(batchSize, "batchSize"),
      validatePositive(bufferMemory.toInt, "bufferMemory")
    )

    val errors = validations.collect { case Left(error) => error }
    if (errors.isEmpty) Right(()) else Left(errors)
  }
}