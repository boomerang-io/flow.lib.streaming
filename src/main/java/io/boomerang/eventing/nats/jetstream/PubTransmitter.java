package io.boomerang.eventing.nats.jetstream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.boomerang.eventing.nats.jetstream.exception.NoNatsConnectionException;
import io.boomerang.eventing.nats.jetstream.exception.StreamNotFoundException;
import io.boomerang.eventing.nats.jetstream.exception.SubjectMismatchException;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
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
public class PubTransmitter implements PubOnlyTunnel {

  private static final Logger logger = LogManager.getLogger(PubTransmitter.class);

  protected final ConnectionPrimer connectionPrimer;

  protected final StreamConfiguration streamConfiguration;

  private final PubOnlyConfiguration pubOnlyConfiguration;

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
  }

  @Override
  public void publish(String subject, String message)
      throws IOException, JetStreamApiException, StreamNotFoundException, SubjectMismatchException {

    // Check if the subject matches stream's wildcard subject
    Boolean subjectMatches = streamConfiguration.getSubjects().stream()
        .anyMatch(wildcard -> SubjectMatchChecker.doSubjectsMatch(subject, wildcard));

    if (!subjectMatches) {
      throw new SubjectMismatchException(
          "Subject \"" + subject + "\" does not match any subjects of the stream!");
    }

    // Set up Retry instance for failed NATS connection
    RetryConfig config = RetryConfig.custom().retryExceptions(NullPointerException.class)
        .waitDuration(Duration.ofMillis(1000)).build();
    RetryRegistry registry = RetryRegistry.of(config);
    Retry retry = registry.retry("retryConnect", config);

    // Get NATS connection
    Connection connection = connectionPrimer.getActiveConnection();

    while (true) {
      if (connection == null) {
        try {
          connection = retry.executeSupplier(this.connectionPrimer::getActiveConnection);
        } catch (NullPointerException npe) {
          throw new NoNatsConnectionException("No connection to the NATS server!");
        }
      }

      if (connection != null) {
        System.out.println("Connected!");
        break;
      }
    }

    // Get Jetstream stream from the NATS server (this will also automatically create the stream if
    // not present on the server)
      StreamInfo streamInfo = StreamManager.getStreamInfo(connection, streamConfiguration,
          pubOnlyConfiguration.isAutomaticallyCreateStream());

      if (streamInfo == null) {
        throw new StreamNotFoundException("Stream could not be found! Consider enabling "
            + "`automaticallyCreateStream` in `PubOnlyConfiguration`");
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

    System.out.println("Message \"" + message + "\" with subject \"" + subject + "\" published to stream: " + publishAck);
    logger.debug("Message published to the stream! " + publishAck);
  }
}
