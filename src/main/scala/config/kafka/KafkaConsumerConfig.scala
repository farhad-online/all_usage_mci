package bigdata.dwbi.mci
package config.kafka

import config.{ConfigValidation, CustomProperties}
import utils.logger.CustomLogger

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties

case class KafkaConsumerConfig(
                                bootstrapServers: String,
                                groupId: String,
                                autoOffsetReset: String = "latest",
                                enableAutoCommit: Boolean = false,
                                maxPollRecords: Int = 500,
                                sessionTimeoutMs: Int = 30000,
                                keyDeserializer: String = classOf[StringDeserializer].getName,
                                valueDeserializer: String = classOf[StringDeserializer].getName,
                                topics: List[String] = List.empty,
                                customProperties: Map[String, String] = Map.empty
                              ) extends ConfigValidation with CustomProperties[KafkaConsumerConfig] with CustomLogger {

  def toKafkaProperties: Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit.toString)
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs.toString)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer)
    customProperties.foreach { case (key, value) =>
      props.put(key, value)
    }
    props
  }

  override def validate(): Either[List[String], Unit] = {
    val validations = List(
      validateNonEmpty(bootstrapServers, "bootstrapServers"),
      validateNonEmpty(groupId, "groupId"),
      validatePositive(maxPollRecords, "maxPollRecords"),
      validatePositive(sessionTimeoutMs, "sessionTimeoutMs"),
      if (topics.nonEmpty) Right(()) else Left("topics list cannot be empty")
    )

    val errors = validations.collect { case Left(error) => error }
    if (errors.isEmpty) Right(()) else Left(errors)
  }
}
