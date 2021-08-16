package io.boomerang.jetstream;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

@Component
class StreamManager {

  private static final Logger logger = LogManager.getLogger(StreamManager.class);

  @Autowired
  Properties properties;

  /**
   * This helper method created and returns a new Jetstream stream.
   * 
   * @category Helper method.
   * @throws JetStreamApiException
   * @throws IOException
   * @return A new {@link io.nats.client.api.StreamInfo StreamInfo} object.
   */
  StreamInfo createNewStream(JetStreamManagement jetStreamManagement)
      throws IOException, JetStreamApiException {

    // @formatter:off
    StreamConfiguration streamConfiguration = StreamConfiguration.builder()
        .name(properties.getStreamName())
        .storageType(properties.getStorageType())
        .subjects(properties.getSubjects())
        .replicas(properties.getReplicas())
        .maxAge(properties.getMessageMaxAge())
        .maxBytes(properties.getMaxBytes())
        .maxMessages(properties.getMaxMessages())
        .maxMsgSize(properties.getMaxMessageSize())
        .maxConsumers(properties.getMaxConsumers())
        .noAck(properties.getNoAcknowledgment())
        .retentionPolicy(properties.getRetentionPolicy())
        .discardPolicy(properties.getDiscardPolicy())
        .duplicateWindow(properties.getDuplicateWindow())
        .build();
    // @formatter:on

    logger.debug("Initializing a new Jetstream stream with configuration: " + streamConfiguration);

    return jetStreamManagement.addStream(streamConfiguration);
  }

  StreamInfo getStreamInfo(JetStreamManagement jetStreamManagement)
      throws IOException, JetStreamApiException {

    try {
      return jetStreamManagement.getStreamInfo(properties.getStreamName());
    } catch (JetStreamApiException e) {

      if (e.getErrorCode() == 404) {
        return null;
      } else {
        throw e;
      }
    }
  }

  boolean streamExists(JetStreamManagement jetStreamManagement) {
    try {
      return getStreamInfo(jetStreamManagement) != null;
    } catch (Exception e) {
      logger.error("An error occurred while retrieving Jetstream stream information!", e);
      return false;
    }
  }
}
