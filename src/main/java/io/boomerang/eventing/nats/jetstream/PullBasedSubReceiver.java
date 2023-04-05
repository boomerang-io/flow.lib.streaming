package io.boomerang.eventing.nats.jetstream;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.boomerang.eventing.nats.jetstream.exception.MisconfigurationException;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamStatusException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;

/**
 * Pull-based subscribe receiver class is responsible for managing connection and various properties
 * for the NATS Jetstream {@code Stream} and {@code Consumer}. It can subscribe and listen for new
 * messages from a NATS Jetstream pull-based {@code Consumer}.
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
public class PullBasedSubReceiver extends SubReceiver {

  public final Integer CONSUMER_PULL_BATCH_SIZE = 50;

  public final Duration CONSUMER_PULL_BATCH_FIRST_MESSAGE_WAIT = Duration.ofSeconds(30);

  private static final Logger logger = LogManager.getLogger(PullBasedSubReceiver.class);

  private JetStreamSubscription jetstreamSubscription;

  private Thread pullSubscriptionThread = null;
  private boolean stopConsumerSub = false;

  /**
   * Create a new {@link PullBasedSubReceiver} object.
   * 
   * @param connectionPrimer Connection primer object.
   * @param streamConfiguration NATS Jetstream {@code Stream} configuration.
   * @param consumerConfiguration NATS Jetstream {@code Consumer} configuration.
   * @param pubSubConfiguration {@link PubSubConfiguration} object.
   * @since 0.3.0
   */
  public PullBasedSubReceiver(ConnectionPrimer connectionPrimer,
      StreamConfiguration streamConfiguration, ConsumerConfiguration consumerConfiguration,
      PubSubConfiguration pubSubConfiguration) {
    super(connectionPrimer, streamConfiguration, consumerConfiguration, pubSubConfiguration);

    ConsumerType consumerType = ConsumerManager.getConsumerType(consumerConfiguration);

    if (consumerType != ConsumerType.PullBased) {
      throw new MisconfigurationException(
          "Pull-based receiver requires a configuration for a pull-based consumer. " + consumerType
              + " was received instead.");
    }
  }

  @Override
  final protected void startConsumerSubscription(Connection connection)
      throws IOException, JetStreamApiException {

    // Create pull subscription options and subscriber itself
    PullSubscribeOptions options = PullSubscribeOptions.bind(streamConfiguration.getName(),
        consumerConfiguration.getDurable());
    jetstreamSubscription = connection.jetStream().subscribe(null, options);

    // Define the subscriber class responsible for pulling and processing messages from the NATS
    // server
    class NatsPullBasedSubscriber implements Runnable {

      private Consumer<Message> consumer;

      NatsPullBasedSubscriber(Consumer<Message> consumer) {
        this.consumer = consumer;
        stopConsumerSub = false;
      }

      @Override
      public void run() {
        logger.debug("Handler thread for Jetstream pull-based consumer is running...");

        // Loop until this thread is interrupted (under normal circumstances means unsubscription)
        // stopConsumerSub is used to control the termination of the thread, even if the thread
        // hangs and isInterrupted() does not return true, when stop is programmatically called
        while (Thread.currentThread().isInterrupted() == false && !stopConsumerSub) {
          try {
            // Get new messages (if any, otherwise wait), then send it to the subscription
            // handler
            jetstreamSubscription
                .iterate(CONSUMER_PULL_BATCH_SIZE, CONSUMER_PULL_BATCH_FIRST_MESSAGE_WAIT)
                .forEachRemaining(consumer);
          } catch (JetStreamStatusException jsse) {
            // jnats throws an exception when receiving a status message with one of the following
            // status codes: 404, 408.
            logger.error("An exception was raised when pulling new messages from the consumer!",
                jsse);
          } catch (IllegalStateException e) {
            logger.error("An exception was raised when pulling new messages from the consumer!", e);
            return;
          }
        }
      }
    };

    // Create and start the thread with the above defined subscriber
    pullSubscriptionThread = new Thread(new NatsPullBasedSubscriber(this::processNewMessage));
    pullSubscriptionThread.start();

    logger.debug("Successfully subscribed to NATS Jetstream consumer! " + jetstreamSubscription);
  }

  @Override
  final protected void stopConsumerSubscription(Connection connection) {

    try {
      stopConsumerSub = true;
      pullSubscriptionThread.interrupt();
      pullSubscriptionThread = null;

      jetstreamSubscription.unsubscribe();
      jetstreamSubscription = null;
    } catch (Exception e) {
      logger.debug(
          "An exception was raised when \"unsubscribe()\" method was invoked for \"jetstreamSubscription\": "
              + e.getLocalizedMessage());
    }
  }

  @Override
  final public Boolean isSubscriptionActive() {
    return super.isSubscribed() && jetstreamSubscription != null
        && pullSubscriptionThread.isAlive();
  }
}
