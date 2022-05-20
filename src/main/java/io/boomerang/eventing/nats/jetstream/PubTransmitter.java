package io.boomerang.eventing.nats.jetstream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.boomerang.eventing.nats.ConnectionPrimerListener;
import io.boomerang.eventing.nats.jetstream.exception.FailedPublishMessageException;
import io.boomerang.eventing.nats.jetstream.exception.StreamNotFoundException;
import io.boomerang.eventing.nats.jetstream.exception.SubjectMismatchException;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.NatsMessage;

/**
 * Publish-only transmitter class is responsible for managing connection and various properties for
 * the NATS Jetstream {@code Stream} and can only publish new messages to it.
 * 
 * @since 0.2.0
 * 
 * @note NATS Jetstream {@code Stream} will be automatically created if {@code PubOnlyConfiguration}
 *       {@link PubOnlyConfiguration#isAutomaticallyCreateStream isAutomaticallyCreateStream()}
 *       property is set to {@code true}. Otherwise, {@link PubTransmitter} will try to find the
 *       NATS Jetstream {@code Stream} by stream configuration's {@link StreamConfiguration#getName
 *       name}.
 */
public class PubTransmitter implements PubOnlyTunnel, ConnectionPrimerListener {

  private static final Logger logger = LogManager.getLogger(PubTransmitter.class);

  protected final ConnectionPrimer connectionPrimer;

  protected final StreamConfiguration streamConfiguration;

  private final PubOnlyConfiguration pubOnlyConfiguration;

  private Queue<Message> failedMessages = new ConcurrentLinkedQueue<>();

  /**
   * Create a new {@link PubTransmitter} object with default configuration properties.
   * 
   * @param connectionPrimer Connection primer object.
   * @param streamConfiguration NATS Jetstream {@code Stream} configuration.
   * @since 0.2.0
   */
  public PubTransmitter(ConnectionPrimer connectionPrimer,
      StreamConfiguration streamConfiguration) {
    this(connectionPrimer, streamConfiguration, new PubOnlyConfiguration.Builder().build());
  }

  /**
   * Create a new {@link PubTransmitter} object.
   * 
   * @param connectionPrimer Connection primer object.
   * @param streamConfiguration NATS Jetstream {@code Stream} configuration.
   * @param pubOnlyConfiguration {@link PubOnlyConfiguration} object.
   * @since 0.2.0
   */
  public PubTransmitter(ConnectionPrimer connectionPrimer, StreamConfiguration streamConfiguration,
      PubOnlyConfiguration pubOnlyConfiguration) {
    this.connectionPrimer = connectionPrimer;
    this.streamConfiguration = streamConfiguration;
    this.pubOnlyConfiguration = pubOnlyConfiguration;
    this.connectionPrimer.addListener(this);
  }

  @Override
  public void publish(String subject, String message) {
    publish(subject, message, false);
  }

  @Override
  public void publish(String subject, String message, Boolean republishOnFail) {

    // Check if the subject matches stream's wildcard subject
    Boolean subjectMatches = streamConfiguration.getSubjects().stream()
        .anyMatch(wildcard -> SubjectMatchChecker.doSubjectsMatch(subject, wildcard));
    if (Boolean.FALSE.equals(subjectMatches)) {
      throw new SubjectMismatchException(
          "Subject \"" + subject + "\" does not match any subjects of the stream!");
    }

    // @formatter:off
    NatsMessage natsMessage = NatsMessage.builder()
        .subject(subject)
        .data(message, StandardCharsets.UTF_8)
        .build();
    // @formatter:on

    Connection connection = connectionPrimer.getActiveConnection();

    if (connection == null) {
      if (Boolean.TRUE.equals(republishOnFail)) {

        // Failed because there is no active connection, store message
        logger.error("Could not publish the message due to connection issues!"
            + " Store it and try to publish once connection is re-established.");
        failedMessages.add(natsMessage);
      } else {
        throw new FailedPublishMessageException("No connection to the NATS server!");
      }
    } else {

      try {
        StreamInfo streamInfo = StreamManager.getStreamInfo(connection, streamConfiguration,
            pubOnlyConfiguration.isAutomaticallyCreateStream());

        // If the there is no Stream on the NATS server and automatically create stream is set to
        // false, can't publish the message thus throw an exception
        if (streamInfo == null) {
          throw new StreamNotFoundException("Stream could not be found! Consider enabling "
              + "`automaticallyCreateStream` in `PubOnlyConfiguration`");
        }

        PublishAck publishAck = connection.jetStream().publish(natsMessage);

        if (publishAck.hasError()) {
          logger.error("Failed to publish the message to the stream! " + publishAck.getError());
        } else {
          logger.debug("Message published to the stream! " + publishAck);
        }
      } catch (IOException e) {

        if (Boolean.TRUE.equals(republishOnFail)) {

          // Failed because there is no active connection, store message
          logger.error("Could not publish the message due to connection issues!"
              + " Store it and try to publish once connection is re-established.");
          failedMessages.add(natsMessage);
        } else {
          throw new FailedPublishMessageException("Connection exception raised!", e);
        }
      } catch (JetStreamApiException e) {
        throw new FailedPublishMessageException("Failed because of a Jetstream exception", e);
      }
    }
  }

  @Override
  public void connectionUpdated(ConnectionPrimer connectionPrimer) {
    Connection connection = connectionPrimer.getActiveConnection();

    if (connection != null) {
      publishFailedMessages(connection);
    }
  }

  private synchronized void publishFailedMessages(Connection connection) {

    if (failedMessages.isEmpty()) {
      return;
    }

    try {
      StreamInfo streamInfo = StreamManager.getStreamInfo(connection, streamConfiguration,
          pubOnlyConfiguration.isAutomaticallyCreateStream());

      // If the there is no Stream on the NATS server and automatically create stream is set to
      // false, can't publish the messages thus fail silently and clear failed messages queue
      if (streamInfo == null) {
        failedMessages.clear();
        return;
      }
    } catch (IOException e) {
      logger.error("Could not publish the message due to connection issues!"
          + " Try to publish the message once connection is re-established.");
      return;

    } catch (JetStreamApiException e) {

      // Failed because of an issue related to Jetstream, fail silently and clear the messages
      logger.error("Failed to re-publish messaged because of an issue related to Jetstream!"
          + " Failed messages will be discarded!");
      failedMessages.clear();
      return;
    }

    while (!failedMessages.isEmpty()) {

      // Publish failed message at head of queue
      try {
        PublishAck publishAck = connection.jetStream().publish(failedMessages.peek());

        if (publishAck.hasError()) {
          logger.error("Failed to publish the message to the stream! " + publishAck.getError());
        } else {
          logger.debug("Message re-published to the stream! " + publishAck);
        }

        failedMessages.poll();

      } catch (IOException e) {

        // Message publish failed due to connection issue, return from the method since there is
        // no reason to continue with publishing other messages
        logger.error("Could not publish the message due to connection issues!"
            + " Try to publish the message once connection is re-established.");
        return;
      } catch (JetStreamApiException e) {

        // Failed because of an issue related to Jetstream, fail silently and drop the message for
        // which the publish was attempted
        logger.error("Failed to re-publish messaged because of an issue related to Jetstream!"
            + " The message will be discarded!");
        failedMessages.poll();
      }
    }
  }
}
