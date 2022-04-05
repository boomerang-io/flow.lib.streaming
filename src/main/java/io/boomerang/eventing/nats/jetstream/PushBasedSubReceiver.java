package io.boomerang.eventing.nats.jetstream;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.boomerang.eventing.nats.jetstream.exception.MisconfigurationException;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;

/**
 * Push-based subscribe receiver class is responsible for managing connection and various properties
 * for the NATS Jetstream {@code Stream} and {@code Consumer}. It can subscribe and listen for new
 * messages from a NATS Jetstream push-based {@code Consumer}.
 * 
 * @since 0.3.0
 * 
 * @note NATS Jetstream {@code Stream} will be automatically created if {@code PubSubConfiguration}
 *       {@link PubSubConfiguration#isAutomaticallyCreateStream isAutomaticallyCreateStream()}
 *       property is set to {@code true}. Otherwise, {@link PubSubTransceiver} will try to find the
 *       NATS Jetstream {@code Stream} by stream configuration's {@link StreamConfiguration#getName
 *       name}.
 * @note NATS Jetstream {@code Consumer} will be automatically created if
 *       {@code PubSubConfiguration} {@link PubSubConfiguration#isAutomaticallyCreateConsumer
 *       isAutomaticallyCreateConsumer()} property is set to {@code true}. Otherwise,
 *       {@link PubSubTransceiver} will try to find the NATS Jetstream {@code Consumer} by consumer
 *       configuration's {@link ConsumerConfiguration#getDurable() durable} name.
 */
class PushBasedSubReceiver extends SubReceiver {

  private static final Logger logger = LogManager.getLogger(PushBasedSubReceiver.class);

  private Dispatcher dispatcher;

  private JetStreamSubscription jetstreamSubscription;

  /**
   * Create a new {@link PushBasedSubReceiver} object.
   * 
   * @param connectionPrimer Connection primer object.
   * @param streamConfiguration NATS Jetstream {@code Stream} configuration.
   * @param consumerConfiguration NATS Jetstream {@code Consumer} configuration.
   * @param pubSubConfiguration {@link PubSubConfiguration} object.
   * @since 0.3.0
   */
  PushBasedSubReceiver(ConnectionPrimer connectionPrimer, StreamConfiguration streamConfiguration,
      ConsumerConfiguration consumerConfiguration, PubSubConfiguration pubSubConfiguration) {
    super(connectionPrimer, streamConfiguration, consumerConfiguration, pubSubConfiguration);

    ConsumerType consumerType = ConsumerManager.getConsumerType(consumerConfiguration);

    if (consumerType != ConsumerType.PushBased) {
      throw new MisconfigurationException(
          "Push-based receiver requires a configuration for a push-based consumer. " + consumerType
              + " was received instead.");
    }
  }

  @Override
  final protected void startConsumerSubscription(Connection connection)
      throws IOException, JetStreamApiException {

    // Create a dispatcher without a default handler and the push subscription
    // options
    dispatcher = connection.createDispatcher();
    PushSubscribeOptions options = PushSubscribeOptions.bind(streamConfiguration.getName(),
        consumerConfiguration.getDurable());

    // Subscribe to receive messages on subject and return this subscription
    jetstreamSubscription =
        connection.jetStream().subscribe(null, dispatcher, this::processNewMessage, false, options);

    logger.debug("Successfully subscribed to NATS Jetstream consumer! " + jetstreamSubscription);
  }

  @Override
  final protected void stopConsumerSubscription(Connection connection) {
    try {
      jetstreamSubscription.unsubscribe();
      jetstreamSubscription = null;

      if (connection != null) {
        connection.closeDispatcher(dispatcher);
        dispatcher = null;
      }
    } catch (Exception e) {
      logger.debug(
          "An exception was raised when \"unsubscribe()\" method was invoked for\"jetstreamSubscription\": "
              + e.getLocalizedMessage());
    }
  }

  @Override
  public Boolean isSubscriptionActive() {
    return super.isSubscribed() && jetstreamSubscription != null
        && jetstreamSubscription.isActive();
  }
}
