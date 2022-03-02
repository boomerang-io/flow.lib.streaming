package io.boomerang.eventing.nats.jetstream;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.boomerang.eventing.nats.ConnectionPrimerListener;
import io.boomerang.eventing.nats.jetstream.exception.AlreadySubscribedException;
import io.boomerang.eventing.nats.jetstream.exception.ConsumerNotFoundException;
import io.boomerang.eventing.nats.jetstream.exception.StreamNotFoundException;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.MessageHandler;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

/**
 * Publish and subscribe transceiver class is responsible for managing connection and various
 * properties for the NATS Jetstream {@code Stream} and {@code Consumer}. It can publish, as well as
 * subscribe for new messages.
 * 
 * @since 0.2.0
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
public class PubSubTransceiver extends PubTransmitter
    implements PubSubTunnel, ConnectionPrimerListener {

  private static final Logger logger = LogManager.getLogger(PubSubTransceiver.class);

  public final Integer CONSUMER_PULL_BATCH_SIZE = 50;

  public final Duration CONSUMER_PULL_BATCH_FIRST_MESSAGE_WAIT = Duration.ofSeconds(30);

  private final ConsumerConfiguration consumerConfiguration;

  private final PubSubConfiguration pubSubConfiguration;

  private Reference<SubHandler> subHandlerRef = new WeakReference<>(null);

  private JetStreamSubscription jetstreamSubscription;

  private AtomicBoolean subscriptionActive = new AtomicBoolean(false);

  /**
   * Create a new {@link PubSubTransceiver} object with default configuration properties.
   * 
   * @param connectionPrimer Connection primer object.
   * @param streamConfiguration NATS Jetstream {@code Stream} configuration.
   * @param consumerConfiguration NATS Jetstream {@code Consumer} configuration.
   * @since 0.2.0
   */
  public PubSubTransceiver(ConnectionPrimer connectionPrimer,
      StreamConfiguration streamConfiguration, ConsumerConfiguration consumerConfiguration) {
    this(connectionPrimer, streamConfiguration, consumerConfiguration,
        new PubSubConfiguration.Builder().build());
  }

  /**
   * Create a new {@link PubSubTransceiver} object.
   * 
   * @param connectionPrimer Connection primer object.
   * @param streamConfiguration NATS Jetstream {@code Stream} configuration.
   * @param consumerConfiguration NATS Jetstream {@code Consumer} configuration.
   * @param pubSubConfiguration {@link PubSubConfiguration} object.
   * @since 0.2.0
   */
  public PubSubTransceiver(ConnectionPrimer connectionPrimer,
      StreamConfiguration streamConfiguration, ConsumerConfiguration consumerConfiguration,
      PubSubConfiguration pubSubConfiguration) {
    // @formatter:off
    super(connectionPrimer, streamConfiguration, new PubOnlyConfiguration.Builder()
        .automaticallyCreateStream(pubSubConfiguration.isAutomaticallyCreateStream())
        .build());
    // @formatter:on
    this.consumerConfiguration = consumerConfiguration;
    this.pubSubConfiguration = pubSubConfiguration;
    this.connectionPrimer.addListener(this);
  }

  @Override
  public void subscribe(SubHandler handler) throws AlreadySubscribedException {

    // Sanity check
    if (isSubscribed()) {
      throw new AlreadySubscribedException("A subscription already exists!");
    }

    // Assign the handler first
    subHandlerRef = new WeakReference<>(handler);

    // Then get NATS connection
    Connection connection = connectionPrimer.getActiveConnection();

    if (connection == null) {

      // If there is no connection, leave subscription as not being active and
      // subscribe once the connection to NATS server is restored
      logger.warn("No NATS server connection! Try subscribing again when connection is restored.");
      return;
    }

    // Lastly, start the subscription
    startConsumerSubscription(connection);
  }

  @Override
  public void unsubscribe() {

    try {
      jetstreamSubscription.unsubscribe();
    } catch (Exception e) {
      logger.debug(
          "An exception was raised when \"unsubscribe()\" method was invoked for \"jetstreamSubscription\": "
              + e.getLocalizedMessage());
    }

    // Unset the subscription
    subHandlerRef = new WeakReference<>(null);
    jetstreamSubscription = null;
    subscriptionActive.set(false);
  }

  @Override
  public Boolean isSubscribed() {
    return subHandlerRef.get() != null;
  }

  @Override
  public Boolean isSubscriptionActive() {
    return isSubscribed() && subscriptionActive.get() && jetstreamSubscription != null;
  }

  @Override
  public void connectionUpdated(ConnectionPrimer connectionPrimer) {

    // If there is a NATS connection and a subscription has been request earlier but
    // hasn't been executed yet (due to no connection to the NATS server), try to
    // subscribe again
    Connection connection = connectionPrimer.getActiveConnection();

    if (isSubscribed() && !isSubscriptionActive() && connection != null) {
      logger.debug("Try to subscribe again: " + connectionPrimer);
      startConsumerSubscription(connection);
    }
  }

  private void startConsumerSubscription(Connection connection) {

    // Set subscription as being active
    subscriptionActive.set(true);

    try {
      // Get Jetstream stream from the NATS server
      StreamInfo streamInfo = StreamManager.getStreamInfo(connection, streamConfiguration,
          pubSubConfiguration.isAutomaticallyCreateStream());

      if (streamInfo == null) {
        throw new StreamNotFoundException("Stream could not be found! Consider enabling "
            + "`automaticallyCreateStream` in `PubSubConfiguration`");
      }

      // Get Jetstream consumer from the NATS server
      ConsumerInfo consumerInfo = ConsumerManager.getConsumerInfo(connection, streamInfo,
          consumerConfiguration, pubSubConfiguration.isAutomaticallyCreateConsumer());

      if (consumerInfo == null) {
        throw new ConsumerNotFoundException("Consumer could not be found! Consider enabling "
            + "`automaticallyCreateConsumer` in `TinCanConfiguration`");
      }

      // Based on the consumer type, start the appropriate subscription
      switch (ConsumerManager.getConsumerType(consumerConfiguration)) {
        case PushBased:
          startPushBasedConsumerSubscription(connection);
          break;
        case PullBased:
          startPullBasedConsumerSubscription(connection);
          break;
      }

      // Subscription successful, notify the handler
      subHandlerRef.get().subscriptionSucceeded(this);

    } catch (Exception e) {

      // Failed to subscribe, invoke the handler failed method and unsubscribe
      SubHandler subHandler = subHandlerRef.get();
      unsubscribe();
      subHandler.subscriptionFailed(this, e);
    }
  }

  private void startPushBasedConsumerSubscription(Connection connection)
      throws IOException, JetStreamApiException {

    // Create the message handler
    MessageHandler handler = (message) -> {
      logger.debug(
          "Handler thread for Jetstream push-based consumer received a new message: " + message);

      if (message != null && subHandlerRef.get() != null) {

        // Acknowledge the message first (prevent slow consumers)
        message.ack();

        // Notify subscription handler
        subHandlerRef.get().newMessageReceived(this, message.getSubject(),
            new String(message.getData()));

      } else {

        // This should not happen!!!
        logger.error(
            "No subscription handler assigned to this communication tunnel! Message not acknowledge!");
      }
    };
    // Create a dispatcher without a default handler and the push subscription
    // options
    Dispatcher dispatcher = connection.createDispatcher();
    PushSubscribeOptions options = PushSubscribeOptions.bind(streamConfiguration.getName(),
        consumerConfiguration.getDurable());

    // Subscribe to receive messages on subject and return this subscription
    jetstreamSubscription =
        connection.jetStream().subscribe(null, dispatcher, handler, false, options);

    logger.debug("Successfully subscribed to NATS Jetstream consumer! " + jetstreamSubscription);
  }

  private void startPullBasedConsumerSubscription(Connection connection)
      throws IOException, JetStreamApiException {

    // Create pull subscription options and subscriber itself
    PullSubscribeOptions options = PullSubscribeOptions.bind(streamConfiguration.getName(),
        consumerConfiguration.getDurable());
    JetStreamSubscription subscription = connection.jetStream().subscribe(null, options);
    PubSubTunnel tunnel = this;
    jetstreamSubscription = subscription;

    // Subscribe!
    Thread handlerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        logger.debug("Handler thread for Jetstream pull-based consumer is running...");

        // TODO Infinite loop - is this risky?
        while (true) {
          try {
            // Get new message (if any, otherwise wait), then send it to the subscription
            // handler
            subscription.iterate(CONSUMER_PULL_BATCH_SIZE, CONSUMER_PULL_BATCH_FIRST_MESSAGE_WAIT)
                .forEachRemaining(message -> {
                  logger.debug(
                      "Handler thread for Jetstream pull-based consumer received a new message: "
                          + message);

                  if (message != null && subHandlerRef.get() != null) {

                    // Notify subscription handler
                    subHandlerRef.get().newMessageReceived(tunnel, message.getSubject(),
                        new String(message.getData()));
                    message.ack();

                  } else {

                    // This should not happen!!!
                    logger.error(
                        "No subscription handler assigned to this communication tunnel! Message not acknowledge!");
                  }
                });
          } catch (IllegalStateException e) {
            logger.info(
                "Handler thread for Jetstream pull-based consumer subscription with durable name \""
                    + consumerConfiguration.getDurable()
                    + "\" stopped! Subscription will be cancelled!");
            tunnel.unsubscribe();
            return;
          }
        }
      }
    });
    handlerThread.start();
    logger.debug("Successfully subscribed to NATS Jetstream consumer! " + subscription);
  }
}
