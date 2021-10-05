package io.boomerang.eventing.nats.jetstream;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.boomerang.eventing.nats.ConnectionPrimer;
import io.boomerang.eventing.nats.ConnectionPrimerListener;
import io.boomerang.eventing.nats.jetstream.exception.ConsumerNotFoundException;
import io.boomerang.eventing.nats.jetstream.exception.NoNatsConnectionException;
import io.boomerang.eventing.nats.jetstream.exception.StreamNotFoundException;
import io.boomerang.eventing.nats.jetstream.exception.SubjectMismatchException;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsMessage;

public class TinCanCommunication implements ConnectionPrimerListener {

  public final Integer CONSUMER_PULL_BATCH_SIZE = 50;

  public final Duration CONSUMER_PULL_BATCH_FIRST_MESSAGE_WAIT = Duration.ofSeconds(30);

  private static final Logger logger = LogManager.getLogger(TinCanCommunication.class);

  private final ConnectionPrimer connectionPrimer;

  private final StreamConfiguration streamConfiguration;

  private final ConsumerConfiguration consumerConfiguration;

  private final TinCanConfiguration tinCanConfiguration;

  private Reference<TinCanMessageListener> messageListenerRef;

  private AtomicBoolean listenerSubscribed = new AtomicBoolean(false);

  public TinCanCommunication(ConnectionPrimer connectionPrimer, StreamConfiguration streamConfiguration,
      ConsumerConfiguration consumerConfiguration) {
    this(connectionPrimer, streamConfiguration, consumerConfiguration, TinCanConfiguration.builder().build());
  }

  public TinCanCommunication(ConnectionPrimer connectionPrimer, StreamConfiguration streamConfiguration,
      ConsumerConfiguration consumerConfiguration, TinCanConfiguration tinCanConfiguration) {
    this.connectionPrimer = connectionPrimer;
    this.streamConfiguration = streamConfiguration;
    this.consumerConfiguration = consumerConfiguration;
    this.tinCanConfiguration = tinCanConfiguration;
  }

  public void publish(String subject, String message)
      throws IOException, JetStreamApiException, StreamNotFoundException {

    // Check if the subject matched stream wildcard subject
    Boolean subjectMatches = streamConfiguration.getSubjects().stream()
        .anyMatch(wildcard -> SubjectMatchChecker.doSubjectsMatch(subject, wildcard));

    if (!subjectMatches) {
      throw new SubjectMismatchException("Subject \"" + subject + "\" does not match any subjects of the stream!");
    }

    // Get NATS connection
    Connection connection = connectionPrimer.getConnection();

    if (connection == null) {
      throw new NoNatsConnectionException("No connection to the NATS server!");
    }

    // Get Jetstream stream from the NATS server
    StreamInfo streamInfo = StreamManager.getStreamInfo(connection, streamConfiguration,
        tinCanConfiguration.isAutomaticallyCreateStream());

    if (streamInfo == null) {
      throw new StreamNotFoundException(
          "Stream could not be found! Consider enabling " + "`automaticallyCreateStream` in `TinCanConfiguration`");
    }

    // Create the NATS message
    // @formatter:off
    Message natsMessage = NatsMessage.builder()
        .subject(subject)
        .data(message, StandardCharsets.UTF_8)
        .build();
    // @formatter:on

    // Publish the message
    PublishAck publishAck = connection.jetStream().publish(natsMessage);

    logger.debug("Message published to the stream! " + publishAck);
  }

  public void subscribe(TinCanMessageListener listener) throws IOException, JetStreamApiException {

    // Assign the listener first
    this.messageListenerRef = new WeakReference<>(listener);
    this.listenerSubscribed.set(true);

    // Get NATS connection
    Connection connection = connectionPrimer.getConnection();

    if (connection == null) {

      // If there is no connection, mark listener not being subscribed and subscribe
      // once the connection is restored
      this.listenerSubscribed.set(false);

      logger.warn("No NATS server connection! Subscribe to listener when connection is restored.");
      return;
    }

    // Get Jetstream stream from the NATS server
    StreamInfo streamInfo = StreamManager.getStreamInfo(connection, streamConfiguration,
        tinCanConfiguration.isAutomaticallyCreateStream());

    if (streamInfo == null) {
      throw new StreamNotFoundException(
          "Stream could not be found! Consider enabling " + "`automaticallyCreateStream` in `TinCanConfiguration`");
    }

    // Get Jetstream consumer from the NATS server
    ConsumerInfo consumerInfo = ConsumerManager.getConsumerInfo(connection, streamInfo, consumerConfiguration,
        tinCanConfiguration.isAutomaticallyCreateConsumer());

    if (consumerInfo == null) {
      throw new ConsumerNotFoundException(
          "Consumer could not be found! Consider enabling " + "`automaticallyCreateConsumer` in `TinCanConfiguration`");
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
        messageListenerRef.get().newMessageReceived(message.getSubject(), new String(message.getData()));

      } else {

        // This should not happen!!!
        throw new NullPointerException(
            "No message listener assigned to this Tin Can communication channel! Message will be lost!");
      }
    };
    // Subscribe to receive messages on subject and return this subscription
    JetStreamSubscription subscription = connection.jetStream().subscribe(">", dispatcher, handler, true, options);

    logger.debug("Successfully subscribed to NATS Jetstream consumer! " + subscription);
  }

  private void startPullBasedConsumerSubscription(Connection connection) throws IOException, JetStreamApiException {

    // Create pull subscription options and subscriber itself
    PullSubscribeOptions options = PullSubscribeOptions.builder().durable(consumerConfiguration.getDurable()).build();
    JetStreamSubscription subscription = connection.jetStream().subscribe(">", options);

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
                    messageListenerRef.get().newMessageReceived(message.getSubject(), new String(message.getData()));

                  } else {

                    // This should not happen!!!
                    throw new NullPointerException(
                        "No message listener assigned to this Tin Can communication channel! Message will be lost!");
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
