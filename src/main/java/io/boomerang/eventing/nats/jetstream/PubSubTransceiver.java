package io.boomerang.eventing.nats.jetstream;

import java.io.IOException;
import java.lang.ref.Reference;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.boomerang.eventing.nats.ConnectionPrimer;
import io.boomerang.eventing.nats.ConnectionPrimerListener;
import io.boomerang.eventing.nats.jetstream.exception.AlreadySubscribedException;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.MessageHandler;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;

/**
 * Publish and subscribe transceiver class is responsible for managing
 * connection and various properties for the NATS Jetstream {@code Stream} and
 * {@code Consumer}. It can publish, as well as subscribe for new messages.
 * 
 * @since 0.2.0
 * 
 * @note NATS Jetstream {@code Stream} will be automatically created if
 *       {@code PubSubConfiguration}
 *       {@link PubSubConfiguration#isAutomaticallyCreateStream
 *       isAutomaticallyCreateStream()} property is set to {@code true}.
 *       Otherwise, {@link PubSubTransceiver} will try to find the NATS
 *       Jetstream {@code Stream} by stream configuration's
 *       {@link StreamConfiguration#getName name}.
 * @note NATS Jetstream {@code Consumer} will be automatically created if
 *       {@code PubSubConfiguration}
 *       {@link PubSubConfiguration#isAutomaticallyCreateConsumer
 *       isAutomaticallyCreateConsumer()} property is set to {@code true}.
 *       Otherwise, {@link PubSubTransceiver} will try to find the NATS
 *       Jetstream {@code Consumer} by consumer configuration's
 *       {@link ConsumerConfiguration#getDurable() durable} name.
 */
public class PubSubTransceiver extends PubTransmitter implements PubSubTunnel, ConnectionPrimerListener {

  private static final Logger logger = LogManager.getLogger(PubSubTransceiver.class);

  public final Integer CONSUMER_PULL_BATCH_SIZE = 50;

  public final Duration CONSUMER_PULL_BATCH_FIRST_MESSAGE_WAIT = Duration.ofSeconds(30);

  private final ConsumerConfiguration consumerConfiguration;

  private final PubSubConfiguration pubSubConfiguration;

  private Reference<SubHandler> messageListenerRef;

  private AtomicBoolean listenerSubscribed = new AtomicBoolean(false);

  /**
   * Create a new {@link PubSubTransceiver} object with default configuration
   * properties.
   * 
   * @param connectionPrimer      Connection primer object.
   * @param streamConfiguration   NATS Jetstream {@code Stream} configuration.
   * @param consumerConfiguration NATS Jetstream {@code Consumer} configuration.
   * @since 0.2.0
   */
  public PubSubTransceiver(ConnectionPrimer connectionPrimer, StreamConfiguration streamConfiguration,
      ConsumerConfiguration consumerConfiguration) {
    this(connectionPrimer, streamConfiguration, consumerConfiguration, new PubSubConfiguration.Builder().build());
  }

  /**
   * Create a new {@link PubSubTransceiver} object.
   * 
   * @param connectionPrimer      Connection primer object.
   * @param streamConfiguration   NATS Jetstream {@code Stream} configuration.
   * @param consumerConfiguration NATS Jetstream {@code Consumer} configuration.
   * @param pubSubConfiguration   {@link PubSubConfiguration} object.
   * @since 0.2.0
   */
  public PubSubTransceiver(ConnectionPrimer connectionPrimer, StreamConfiguration streamConfiguration,
      ConsumerConfiguration consumerConfiguration, PubSubConfiguration pubSubConfiguration) {
    super(connectionPrimer, streamConfiguration, pubSubConfiguration);
    this.consumerConfiguration = consumerConfiguration;
    this.pubSubConfiguration = pubSubConfiguration;
  }

  @Override
  public void subscribe(SubHandler handler) throws AlreadySubscribedException {
    // TODO Auto-generated method stub

  }

  @Override
  public void unsubscribe() {
    // TODO Auto-generated method stub

  }

  @Override
  public Boolean isSubscribed() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void connectionUpdated(ConnectionPrimer connectionPrimer) {
    // TODO Auto-generated method stub

  }

  private void startPushBasedConsumerSubscription(Connection connection) throws IOException, JetStreamApiException {

    // Create a dispatcher without a default handler and the push subscription
    // options
    Dispatcher dispatcher = connection.createDispatcher();
    PushSubscribeOptions options = PushSubscribeOptions.builder().durable(consumerConfiguration.getDurable()).build();

    // Create the message handler
    MessageHandler handler = (message) -> {
      logger.debug("Handler thread for Jetstream push-based consumer received a new message: " + message);

      if (message != null && messageListenerRef.get() != null) {

        // Notify message listener
        messageListenerRef.get().newMessageReceived(this, message.getSubject(), new String(message.getData()));
        message.ack();

      } else {

        // This should not happen!!!
        logger.error("No message listener assigned to this communication tunnel! Message not acknowledge!");
      }
    };
    // Subscribe to receive messages on subject and return this subscription
    JetStreamSubscription subscription = connection.jetStream().subscribe(">", dispatcher, handler, false, options);

    logger.debug("Successfully subscribed to NATS Jetstream consumer! " + subscription);
  }

  private void startPullBasedConsumerSubscription(Connection connection) throws IOException, JetStreamApiException {

    // Create pull subscription options and subscriber itself
    PullSubscribeOptions options = PullSubscribeOptions.builder().durable(consumerConfiguration.getDurable()).build();
    JetStreamSubscription subscription = connection.jetStream().subscribe(">", options);
    PubSubTunnel tunnel = this;

    Thread handlerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        logger.debug("Handler thread for Jetstream pull-based consumer is running...");

        while (true) {
          try {
            // Get new message (if any, otherwise wait), then send it to the listener
            // handler
            subscription.iterate(CONSUMER_PULL_BATCH_SIZE, CONSUMER_PULL_BATCH_FIRST_MESSAGE_WAIT)
                .forEachRemaining(message -> {
                  logger.debug("Handler thread for Jetstream pull-based consumer received a new message: " + message);

                  if (message != null && messageListenerRef.get() != null) {

                    // Notify message listener
                    messageListenerRef.get().newMessageReceived(tunnel, message.getSubject(),
                        new String(message.getData()));
                    message.ack();

                  } else {

                    // This should not happen!!!
                    logger.error(
                        "No message listener assigned to this communication tunnel! Message not acknowledge!");
                  }
                });
          } catch (IllegalStateException e) {
            logger.info("Handler thread for Jetstream pull-based consumer subscription with durable name \""
                + consumerConfiguration.getDurable() + "\" stopped!");
            return;
          }
        }
      }
    });
    handlerThread.start();
    logger.debug("Successfully subscribed to NATS Jetstream consumer! " + subscription);
  }
}
