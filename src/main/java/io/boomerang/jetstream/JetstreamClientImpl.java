package io.boomerang.jetstream;

import java.nio.charset.StandardCharsets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;

@Component
class JetstreamClientImpl implements JetstreamClient {

  private static final Logger logger = LogManager.getLogger(JetstreamClientImpl.class);

  @Autowired
  private Connection natsConnection;

  @Autowired
  private StreamManager streamManager;

  private JetStream jetstreamContext;

  @Override
  public void publish(String subject, String message) {

    // Create the NATS message
    // @formatter:off
    Message natsMessage = NatsMessage.builder()
        .subject(subject)
        .data(message, StandardCharsets.UTF_8)
        .build();
    // @formatter:on

    // Publish the message
    try {
      PublishAck publishAck = getJetstreamContext().publish(natsMessage);
      logger.debug("Message published! " + publishAck);
    } catch (Exception e) {
      logger.error("An error occurred publishing the message to NATS Jetstream server!", e);
    }
  }

  @Override
  public void consume(String subject) {
    // TODO Auto-generated method stub
  }

  /**
   * Lazy instantiates and returns a new {@link io.nats.client.JetStream JetStream} connection
   * context.
   * 
   * @category Getter method.
   */
  private JetStream getJetstreamContext() {

    if (this.jetstreamContext == null) {
      try {
        // Create new Jetstream connection context
        this.jetstreamContext = natsConnection.jetStream();

        // Create the Jetstream stream if this doesn't exist
        if (!streamManager.streamExists(natsConnection.jetStreamManagement())) {
          streamManager.createNewStream(natsConnection.jetStreamManagement());
        }
      } catch (Exception e) {
        logger.error("An error occurred while connecting to NATS Jetstream server!", e);
      }
    }
    return this.jetstreamContext;
  }
}
