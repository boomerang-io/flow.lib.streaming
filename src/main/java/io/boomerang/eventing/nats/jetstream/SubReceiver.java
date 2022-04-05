package io.boomerang.eventing.nats.jetstream;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.boomerang.eventing.nats.ConnectionPrimerListener;
import io.boomerang.eventing.nats.jetstream.exception.AlreadySubscribedException;
import io.boomerang.eventing.nats.jetstream.exception.ConsumerNotFoundException;
import io.boomerang.eventing.nats.jetstream.exception.StreamNotFoundException;
import io.boomerang.eventing.nats.jetstream.exception.SubHandlerReferenceClearedException;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

/**
 * Subscribe receiver is an abstract class that is responsible for managing connection related
 * properties to a NATS Jetstream {@code Stream} and {@code Consumer}.
 * 
 * @since 0.3.0
 * 
 * @note Use {@link PushBasedSubReceiver} to create a new receiver that consumes messages from a
 *       NATS Jetstream push-based {@code Consumer}.
 * @note Use {@link PullBasedSubReceiver} to create a new receiver that consumes messages from a
 *       NATS Jetstream pull-based {@code Consumer}.
 */
public abstract class SubReceiver implements SubOnlyTunnel, ConnectionPrimerListener {

  private static final Logger logger = LogManager.getLogger(PubSubTransceiver.class);

  private final ConnectionPrimer connectionPrimer;

  private AtomicBoolean subscribed = new AtomicBoolean(false);

  private WeakReference<SubHandler> subHandlerRef = new WeakReference<>(null);

  final ConsumerConfiguration consumerConfiguration;

  final StreamConfiguration streamConfiguration;

  final PubSubConfiguration pubSubConfiguration;

  SubReceiver(ConnectionPrimer connectionPrimer, StreamConfiguration streamConfiguration,
      ConsumerConfiguration consumerConfiguration, PubSubConfiguration pubSubConfiguration) {
    this.connectionPrimer = connectionPrimer;
    this.streamConfiguration = streamConfiguration;
    this.consumerConfiguration = consumerConfiguration;
    this.pubSubConfiguration = pubSubConfiguration;

    this.connectionPrimer.addListener(this);
  }

  public void connectionUpdated(ConnectionPrimer connectionPrimer) {

    // If there is a NATS connection and a subscription has been request earlier but
    // hasn't been executed yet (due to no connection to the NATS server), try to
    // subscribe again
    Connection connection = connectionPrimer.getActiveConnection();

    if (isSubscribed() && isSubscriptionActive() == false && connection != null) {
      logger.debug("Re-establishing a lost subscription: " + connectionPrimer);

      try {
        prepareAndStartSubscription(connection);
      } catch (Exception e) {
        logger.error("Could not re-establish the subscription!", e);
      }
    }
  }

  public void subscribe(SubHandler handler)
      throws AlreadySubscribedException, StreamNotFoundException, ConsumerNotFoundException {

    // Sanity check
    if (subscribed.getAndSet(true)) {
      throw new AlreadySubscribedException("A subscription already exists!");
    }

    // Assign the handler, get NATS connection and check if it is active
    subHandlerRef = new WeakReference<>(handler);
    Connection connection = connectionPrimer.getActiveConnection();

    if (connection == null) {

      // If there is no connection, leave subscription as not being active and subscribe once the
      // connection to NATS server is restored
      logger.warn("No NATS server connection! Try subscribing again when connection is restored.");
      return;
    }

    // Prepare and start consumer subscription
    prepareAndStartSubscription(connection);
  }

  public void unsubscribe() {

    // Unset the subscription
    subscribed.set(false);
    subHandlerRef = new WeakReference<>(null);

    // Stop subscription
    stopConsumerSubscription(connectionPrimer.getActiveConnection());
  }

  public Boolean isSubscribed() {
    return subscribed.get();
  }

  /**
   * This helper method is used to prepare the NATS Jetstream environment before subscribing to a
   * consumer. This involves retrieving the {@code Stream} and {@code Consumer} objects or creating
   * them if specified by the configuration properties.
   */
  private void prepareAndStartSubscription(Connection connection) {

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
            + "`automaticallyCreateConsumer` in `PubSubConfiguration`");
      }

      // Try to start the subscription and notify the handler if successful
      startConsumerSubscription(connection);
      subHandlerRef.get().subscriptionSucceeded(this);

    } catch (Exception e) {

      // Failed to subscribe, invoke the handler failed method and unsubscribe
      SubHandler subHandler = subHandlerRef.get();
      unsubscribe();
      subHandler.subscriptionFailed(this, e);
    }
  }

  /**
   * This helper method is used by the subscription entities to process new incoming messages from
   * the NATS server.
   */
  protected void processNewMessage(Message message) {
    logger.debug(
        "Handler thread for Jetstream pull-based consumer received a new message: " + message);

    // Check the subscription handler first
    if (subHandlerRef.get() == null) {

      // Subscription handler has been cleared - unsubscribe
      unsubscribe();
      logger.error(new SubHandlerReferenceClearedException(
          "No subscription handler assigned to this communication tunnel! "
              + "Message not acknowledged and the subscription was cancelled! "
              + "Is the handler persisted with a strong reference?"));
      return;
    }

    try {
      // Notify subscription handler
      subHandlerRef.get().newMessageReceived(this, message.getSubject(),
          new String(message.getData()));

      // Acknowledge the message only after the handler was invoked and no exceptions were raised
      message.ack();

    } catch (Exception e) {
      logger.error("An exception was raised when subscription handler was invoked!", e);
    }
  }

  protected abstract void startConsumerSubscription(Connection connection) throws Exception;

  protected abstract void stopConsumerSubscription(Connection connection);
}
