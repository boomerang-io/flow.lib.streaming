package io.boomerang.jetstream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.Subscription;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;

@Component
class JetstreamClientImpl implements JetstreamClient {

  private static final Logger logger = LogManager.getLogger(JetstreamClientImpl.class);

  @Autowired
  private Properties properties;

  @Autowired
  private NatsDurableConnection natsDurableConnection;

  @Autowired
  private StreamManager streamManager;

  @Autowired
  private ConsumerManager consumerManager;

  private Map<String, Subscription> activeSubscriptions = new HashMap<>();

  @Override
  public Boolean publish(String subject, String message) {

    try {
      // Get NATS connection
      Connection natsConnection = natsDurableConnection.getOptional().orElseThrow();

      // Create the Jetstream stream if this doesn't exist
      if (!streamManager.streamExists(natsConnection.jetStreamManagement())) {
        streamManager.createNewStream(natsConnection.jetStreamManagement());
      }

      // Create the NATS message
      // @formatter:off
      Message natsMessage = NatsMessage.builder()
          .subject(subject)
          .data(message, StandardCharsets.UTF_8)
          .build();
      // @formatter:on

      // Publish the message
      PublishAck publishAck = natsConnection.jetStream().publish(natsMessage);

      logger.debug("Message published! " + publishAck);
      return true;

    } catch (NoSuchElementException e) {

      logger.error("No connection to the NATS server!", e);
      return false;

    } catch (IOException | JetStreamApiException e) {

      logger.error("An error occurred while publishing the message to NATS Jetstream stream!", e);
      return false;
    }
  }

  @Override
  public Boolean subscribe(String subject, ConsumerType consumerType,
      JetstreamMessageListener listener) {

    // Check if there is an active subscription
    if (activeSubscriptions.get(subject) != null) {
      logger.error("There is already an active subscription for subject \"" + subject
          + "\". Please unsubscribe first!");
      return false;
    }

    // Subscribe and add this subscription to active subscriptions if successful
    try {
      switch (consumerType) {
        case PushBased:
          activeSubscriptions.put(subject, pushBasedSubscription(subject, listener).orElseThrow());
          break;
        case PullBased:
          activeSubscriptions.put(subject, pullBasedSubscription(subject, listener).orElseThrow());
          break;
      }

      // Subscription successful
      return true;
    } catch (NoSuchElementException e) {

      // Could not subscribe
      return false;
    }
  }

  @Override
  public Boolean unsubscribe(String subject) {

    // Check if there is an active subscription
    if (activeSubscriptions.get(subject) == null) {
      logger.error("There is no active subscription for subject \"" + subject + "\"!");
      return false;
    }

    // Unsubscribe and remove the subscription from active subscriptions
    activeSubscriptions.get(subject).unsubscribe();
    activeSubscriptions.remove(subject);

    logger.debug("Unsubscribed from events with subject \"" + subject + "\"!");

    return true;
  };

  private Optional<Subscription> pushBasedSubscription(String subject,
      JetstreamMessageListener listener) {

    try {
      // Get NATS connection
      Connection natsConnection = natsDurableConnection.getOptional().orElseThrow();

      // Get push-based consumer first
      ConsumerInfo consumerInfo = consumerManager
          .getConsumerInfo(natsConnection.jetStreamManagement(), ConsumerType.PushBased);

      // Create the Jetstream consumer if this doesn't exist
      if (consumerInfo == null) {
        consumerInfo = consumerManager.createNewConsumer(natsConnection.jetStreamManagement(),
            ConsumerType.PushBased);
      }

      // Create a dispatcher without a default handler and the push subscription options
      Dispatcher dispatcher = natsConnection.createDispatcher();
      PushSubscribeOptions options =
          PushSubscribeOptions.builder().durable(consumerInfo.getName()).build();

      // Create the message handler
      MessageHandler handler = (message) -> {
        String data = new String(message.getData());
        listener.newMessageReceived(message.getSubject(), data);
        message.ack();
      };

      // Subscribe to receive messages on subject and return this subscription
      JetStreamSubscription subscription =
          natsConnection.jetStream().subscribe(subject, dispatcher, handler, false, options);
      logger.debug("Successfully subscribed to NATS Jetstream consumer! " + subscription);
      return Optional.of(subscription);

    } catch (NoSuchElementException e) {

      logger.error("No connection to the NATS server!", e);
      return Optional.empty();

    } catch (IOException | JetStreamApiException e) {

      logger.error("An error occurred while subscribing to NATS Jetstream consumer!", e);
      return Optional.empty();
    }
  }

  private Optional<Subscription> pullBasedSubscription(String subject,
      JetstreamMessageListener listener) {

    try {
      // Get NATS connection
      Connection natsConnection = natsDurableConnection.getOptional().orElseThrow();

      // Get pull-based consumer first
      ConsumerInfo consumerInfo = consumerManager
          .getConsumerInfo(natsConnection.jetStreamManagement(), ConsumerType.PullBased);

      // Create the Jetstream consumer if this doesn't exist
      if (consumerInfo == null) {
        consumerInfo = consumerManager.createNewConsumer(natsConnection.jetStreamManagement(),
            ConsumerType.PullBased);
      }

      // Create pull subscription options and subscriber itself
      PullSubscribeOptions options =
          PullSubscribeOptions.builder().durable(consumerInfo.getName()).build();
      JetStreamSubscription subscription = natsConnection.jetStream().subscribe(subject, options);

      Thread handlerThread = new Thread(new Runnable() {
        @Override
        public void run() {
          logger.debug("Handler thread for Jetstream pull-based consumer is running...");

          while (true) {
            try {

              // Get new message (if any, otherwise wait), then send it to the listener handler
              subscription
                  .iterate(properties.getConsumerPullBatchSize(),
                      properties.getConsumerPullBatchFirstMessageWait())
                  .forEachRemaining(message -> {

                    logger.debug(
                        "Handler thread for Jetstream pull-based consumer received a new message: "
                            + message);

                    if (message != null) {
                      String data = new String(message.getData());
                      listener.newMessageReceived(message.getSubject(), data);
                      message.ack();
                    }
                  });
            } catch (IllegalStateException e) {
              logger.info(
                  "Handler thread for Jetstream pull-based consumer subscription with subject \""
                      + subject + "\" stopped!");
              return;
            }
          }
        }
      });

      handlerThread.start();
      logger.debug("Successfully subscribed to NATS Jetstream consumer! " + subscription);
      return Optional.of(subscription);

    } catch (NoSuchElementException e) {

      logger.error("No connection to the NATS server!", e);
      return Optional.empty();

    } catch (IOException | JetStreamApiException e) {

      logger.error("An error occurred while subscribing to NATS Jetstream consumer!", e);
      return Optional.empty();
    }
  }
}
