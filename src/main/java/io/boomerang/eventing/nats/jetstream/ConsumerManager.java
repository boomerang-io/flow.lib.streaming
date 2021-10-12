package io.boomerang.eventing.nats.jetstream;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StreamInfo;

class ConsumerManager {

  private static final Logger logger = LogManager.getLogger(ConsumerManager.class);

  /**
   * This helper method creates and returns a new Jetstream consumer.
   * 
   * @param connection            NATS server {@link io.nats.client.Connection
   *                              Connection} object.
   * @param streamInfo            {@link io.nats.client.api.StreamInfo StreamInfo}
   *                              object referencing an existing Jetstream stream.
   * @param consumerConfiguration {@link io.nats.client.api.ConsumerConfiguration
   *                              ConsumerConfiguration} for creating the new
   *                              Consumer.
   * @throws JetStreamApiException
   * @throws IOException
   * @return A new {@link io.nats.client.api.ConsumerInfo ConsumerInfo} object.
   */
  static ConsumerInfo createNewConsumer(Connection connection, StreamInfo streamInfo,
      ConsumerConfiguration consumerConfiguration) throws IOException, JetStreamApiException {

    logger.debug("Initializing a new Jetstream consumer of type \"" + getConsumerType(consumerConfiguration)
        + "\" with configuration: " + consumerConfiguration);

    // Create and return Jetstream consumer
    return connection.jetStreamManagement().addOrUpdateConsumer(streamInfo.getConfiguration().getName(),
        consumerConfiguration);
  }

  /**
   * This helper method returns information about a Jetstream consumer if this
   * exists, {@code null} otherwise.
   * 
   * @param connection            NATS server {@link io.nats.client.Connection
   *                              Connection} object.
   * @param streamInfo            {@link io.nats.client.api.StreamInfo StreamInfo}
   *                              object referencing an existing Jetstream stream.
   * @param consumerConfiguration {@link io.nats.client.api.ConsumerConfiguration
   *                              ConsumerConfiguration} for creating the new
   *                              Consumer.
   * @throws JetStreamApiException
   * @throws IOException
   * @return Consumer information. See {@link io.nats.client.api.ConsumerInfo
   *         ConsumerInfo}.
   */
  static ConsumerInfo getConsumerInfo(Connection connection, StreamInfo streamInfo,
      ConsumerConfiguration consumerConfiguration) throws IOException, JetStreamApiException {
    return getConsumerInfo(connection, streamInfo, consumerConfiguration, false);
  }

  /**
   * This helper method returns information about a Jetstream consumer if this
   * exists or has been created, {@code null} otherwise.
   * 
   * @param connection            NATS server {@link io.nats.client.Connection
   *                              Connection} object.
   * @param streamInfo            {@link io.nats.client.api.StreamInfo StreamInfo}
   *                              object referencing an existing Jetstream stream.
   * @param consumerConfiguration {@link io.nats.client.api.ConsumerConfiguration
   *                              ConsumerConfiguration} for creating the new
   *                              Consumer.
   * @param createIfMissing       Set to {@code true} to try to create a new
   *                              Consumer if this can't be found on the NATS
   *                              server.
   * @throws JetStreamApiException
   * @throws IOException
   * @return Consumer information. See {@link io.nats.client.api.ConsumerInfo
   *         ConsumerInfo}.
   */
  static ConsumerInfo getConsumerInfo(Connection connection, StreamInfo streamInfo,
      ConsumerConfiguration consumerConfiguration, Boolean createIfMissing) throws IOException, JetStreamApiException {

    try {
      return connection.jetStreamManagement().getConsumerInfo(streamInfo.getConfiguration().getName(),
          consumerConfiguration.getDurable());
    } catch (JetStreamApiException e) {

      if (e.getErrorCode() == 404) {
        return createIfMissing ? createNewConsumer(connection, streamInfo, consumerConfiguration) : null;
      } else {
        throw e;
      }
    }
  }

  /**
   * This helper method returns {@code true} if a consumer of with the requested
   * configuration exists. {@code false} otherwise.
   * 
   * @param connection            NATS server {@link io.nats.client.Connection
   *                              Connection} object.
   * @param streamInfo            {@link io.nats.client.api.StreamInfo StreamInfo}
   *                              object referencing an existing Jetstream stream.
   * @param consumerConfiguration {@link io.nats.client.api.ConsumerConfiguration
   *                              ConsumerConfiguration} for creating the new
   *                              Consumer.
   * @return {@link java.lang.Boolean Boolean}.
   */
  static Boolean consumerExists(Connection connection, StreamInfo streamInfo,
      ConsumerConfiguration consumerConfiguration) {
    try {
      return getConsumerInfo(connection, streamInfo, consumerConfiguration) != null;
    } catch (Exception e) {
      logger.error("An error occurred while retrieving Jetstream consumer information!", e);
      return false;
    }
  }

  /**
   * This helper method returns the Consumer type based on provided
   * {@link io.nats.client.api.ConsumerConfiguration ConsumerConfiguration}.
   *
   * @param consumerConfiguration {@link io.nats.client.api.ConsumerConfiguration
   *                              ConsumerConfiguration} to retrieve
   *                              {@link ConsumerType ConsumerType}
   * @return A {@link ConsumerType ConsumerType} object.
   */
  static ConsumerType getConsumerType(ConsumerConfiguration consumerConfiguration) {
    if (Strings.isBlank(consumerConfiguration.getDeliverSubject())) {
      return ConsumerType.PullBased;
    } else {
      return ConsumerType.PushBased;
    }
  }
}
