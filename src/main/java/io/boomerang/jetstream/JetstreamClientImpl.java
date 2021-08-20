package io.boomerang.jetstream;

import java.nio.charset.StandardCharsets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;

@Component
class JetstreamClientImpl implements JetstreamClient {

  private static final Logger logger = LogManager.getLogger(JetstreamClientImpl.class);

  @Autowired
  private Connection natsConnection;

  @Autowired
  private StreamManager streamManager;

  @Autowired
  private ConsumerManager consumerManager;

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
  public Boolean subscribe(String subject, JetstreamMessageListener listener) {

    try {
      ConsumerInfo consumerInfo =
          consumerManager.getConsumerInfo(natsConnection.jetStreamManagement());

      // Create the Jetstream consumer if this doesn't exist
      if (consumerInfo == null) {
        consumerInfo = consumerManager.createNewConsumer(natsConnection.jetStreamManagement());
      }

      // Create a dispatcher without a default handler and the push subscription options
      Dispatcher dispatcher = natsConnection.createDispatcher();
      PushSubscribeOptions options =
          PushSubscribeOptions.builder().durable(consumerInfo.getName()).build();

      // Create our message handler
      MessageHandler handler = (message) -> {
        String data = new String(message.getData());
        listener.newMessageReceived(message.getSubject(), data);
        message.ack();
      };

      JetStreamSubscription subscription =
          natsConnection.jetStream().subscribe(subject, dispatcher, handler, false, options);

      logger.debug("Successfully subscribed to NATS Jetstream consumer! " + subscription);

      return true;
    } catch (Exception e) {

      logger.error("An error occurred while subscribing to NATS Jetstream consumer!", e);

      return false;
    }
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
