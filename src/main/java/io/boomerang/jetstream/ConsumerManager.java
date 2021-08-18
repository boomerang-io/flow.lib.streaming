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
   * @throws JetStreamApiException
   * @throws IOException
   * @return A new {@link io.nats.client.api.ConsumerInfo ConsumerInfo} object.
   */
  ConsumerInfo createNewConsumer(JetStreamManagement jetStreamManagement)
      throws IOException, JetStreamApiException {

    // @formatter:off
    ConsumerConfiguration consumerConfiguration = ConsumerConfiguration.builder()
        .durable(properties.getConsumerName())
        .deliverSubject(properties.getConsumerDeliveryTarget())
        .deliverPolicy(properties.getConsumerDeliverPolicy())
        .ackPolicy(properties.getConsumerAcknowledgmentPolicy())
        .ackWait(properties.getConsumerAcknowledgmentTimeout())
        .replayPolicy(properties.getConsumerReplayPolicy())
        .filterSubject(properties.getConsumerFilterSubject())
        .maxAckPending(properties.getConsumerMaxAcknowledgmentMessagesPending())
        .maxDeliver(properties.getConsumerMaxDeliverRetry())
        .build();
    // @formatter:on

    // Create the Jetstream stream if this doesn't exist
    if (!streamManager.streamExists(jetStreamManagement)) {
      streamManager.createNewStream(jetStreamManagement);
    }

    // Create Jetstream consumer
    logger.debug(
        "Initializing a new Jetstream consumer with configuration: " + consumerConfiguration);

    return jetStreamManagement.addOrUpdateConsumer(properties.getStreamName(),
        consumerConfiguration);
  }

  ConsumerInfo getConsumerInfo(JetStreamManagement jetStreamManagement)
      throws IOException, JetStreamApiException {

    try {
      return jetStreamManagement.getConsumerInfo(properties.getStreamName(),
          properties.getConsumerName());
    } catch (JetStreamApiException e) {

      if (e.getErrorCode() == 404) {
        return null;
      } else {
        throw e;
      }
    }
  }

  Boolean consumerExists(JetStreamManagement jetStreamManagement) {
    try {
      return getConsumerInfo(jetStreamManagement) != null;
    } catch (Exception e) {
      logger.error("An error occurred while retrieving Jetstream consumer information!", e);
      return false;
    }
  }
}
