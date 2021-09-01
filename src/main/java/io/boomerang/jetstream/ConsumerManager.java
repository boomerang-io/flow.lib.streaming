package io.boomerang.jetstream;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;

@Component
class ConsumerManager {

  private static final Logger logger = LogManager.getLogger(ConsumerManager.class);

  @Autowired
  Properties properties;

  @Autowired
  StreamManager streamManager;

  /**
   * This helper method creates and returns a new Jetstream consumer.
   * 
   * @category Helper method.
   * @param jetStreamManagement Jetstream management context to be used when creating a new
   *        consumer.
   * @param consumerType The type of consumer to be created. See
   *        {@link io.boomerang.jetstream.ConsumerType ConsumerType}.
   * @throws JetStreamApiException
   * @throws IOException
   * @return A new {@link io.nats.client.api.ConsumerInfo ConsumerInfo} object.
   */
  ConsumerInfo createNewConsumer(JetStreamManagement jetStreamManagement, ConsumerType consumerType)
      throws IOException, JetStreamApiException {

    // Create the configuration for consumer first
    ConsumerConfiguration consumerConfiguration = null;

    switch (consumerType) {
      case PushBased:

        // @formatter:off
        consumerConfiguration = ConsumerConfiguration.builder()
            .durable(properties.getConsumerPushName())
            .ackPolicy(properties.getConsumerPushAcknowledgmentPolicy())
            .ackWait(properties.getConsumerPushAcknowledgmentWait())
            .deliverPolicy(properties.getConsumerPushDeliverPolicy())
            .deliverSubject(properties.getConsumerPushDeliverySubject())
            .filterSubject(properties.getConsumerPushFilterSubject())
            .maxAckPending(properties.getConsumerPushMaxAcknowledgmentsPending())
            .maxDeliver(properties.getConsumerPushMaxDeliverRetry())
            .replayPolicy(properties.getConsumerPushReplayPolicy())
            .build();
        // @formatter:on
        break;

      case PullBased:

        // @formatter:off
        consumerConfiguration = ConsumerConfiguration.builder()
            .durable(properties.getConsumerPullName())
            .ackPolicy(properties.getConsumerPullAcknowledgmentPolicy())
            .ackWait(properties.getConsumerPullAcknowledgmentWait())
            .deliverPolicy(properties.getConsumerPullDeliverPolicy())
            .filterSubject(properties.getConsumerPullFilterSubject())
            .maxAckPending(properties.getConsumerPullMaxAcknowledgmentsPending())
            .maxDeliver(properties.getConsumerPullMaxDeliverRetry())
            .replayPolicy(properties.getConsumerPullReplayPolicy())
            .build();
        // @formatter:on
        break;
    }

    // Create the Jetstream stream if this doesn't exist
    if (!streamManager.streamExists(jetStreamManagement)) {
      streamManager.createNewStream(jetStreamManagement);
    }

    // Create Jetstream consumer
    logger.debug("Initializing a new Jetstream consumer of type \"" + consumerType
        + "\" with configuration: " + consumerConfiguration);

    return jetStreamManagement.addOrUpdateConsumer(properties.getStreamName(),
        consumerConfiguration);
  }

  /**
   * This helper method returns information about a Jetstream consumer.
   * 
   * @category Helper method.
   * @param jetStreamManagement Jetstream management context to be used when looking up for
   *        consumer.
   * @param consumerType The type of consumer to get information about. See
   *        {@link io.boomerang.jetstream.ConsumerType ConsumerType}.
   * @throws JetStreamApiException
   * @throws IOException
   * @return Consumer information. See {@link io.nats.client.api.ConsumerInfo ConsumerInfo}.
   */
  ConsumerInfo getConsumerInfo(JetStreamManagement jetStreamManagement, ConsumerType consumerType)
      throws IOException, JetStreamApiException {

    String consumerName = (consumerType == ConsumerType.PushBased ? properties.getConsumerPushName()
        : properties.getConsumerPullName());

    try {
      return jetStreamManagement.getConsumerInfo(properties.getStreamName(), consumerName);
    } catch (JetStreamApiException e) {

      if (e.getErrorCode() == 404) {
        return null;
      } else {
        throw e;
      }
    }
  }

  /**
   * This helper method returns {@code true} if a consumer of requested type exists. {@code false}
   * otherwise.
   * 
   * @category Helper method.
   * @param jetStreamManagement Jetstream management context to be used when looking up for
   *        consumer.
   * @param consumerType The type of consumer. See {@link io.boomerang.jetstream.ConsumerType
   *        ConsumerType}.
   * @return {@link java.lang.Boolean Boolean}.
   */
  Boolean consumerExists(JetStreamManagement jetStreamManagement, ConsumerType consumerType) {
    try {
      return getConsumerInfo(jetStreamManagement, consumerType) != null;
    } catch (Exception e) {
      logger.error("An error occurred while retrieving Jetstream consumer information!", e);
      return false;
    }
  }
}
