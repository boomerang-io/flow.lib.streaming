package io.boomerang.eventing.nats.jetstream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.boomerang.eventing.nats.ConnectionPrimer;
import io.boomerang.eventing.nats.ConnectionPrimerListener;
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

  private ConcurrentLinkedQueue<Message> failedMessages = new ConcurrentLinkedQueue<>(); 

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

  // TODO remove all System.out.printlns after testing
  @Override
  public void publish(String subject, String message)
      throws IOException, JetStreamApiException, StreamNotFoundException, SubjectMismatchException {

    // Check if the subject matches stream's wildcard subject
    Boolean subjectMatches = streamConfiguration.getSubjects().stream()
        .anyMatch(wildcard -> SubjectMatchChecker.doSubjectsMatch(subject, wildcard));
    if (Boolean.FALSE.equals(subjectMatches)) {
      throw new SubjectMismatchException(
          "Subject \"" + subject + "\" does not match any subjects of the stream!");
    }

    // Get NATS connection
    Connection connection = connectionPrimer.getActiveConnection();
    try {
      // Get Jetstream stream from the NATS server (this will also automatically create the stream
      // if not present on the server)
      getJetStream(connection);

      // Create the NATS message
      Message natsMessage = createNATSMessage(subject, message);

      // Publish the message
      PublishAck publishAck = connection.jetStream().publish(natsMessage);
      
      System.out.println("Message \"" + message + "\" with subject \"" + subject + "\" published to stream");
      printObject(publishAck);
      logger.debug("Message published to the stream! " + publishAck);
    } catch (IOException | JetStreamApiException | NullPointerException e) {
      System.out.println("Connection to NATS server failed, storing message to send later");
      failedMessages.add(createNATSMessage(subject, message));
      printObject(failedMessages);
    }
    
  }

  private void getJetStream(Connection connection) throws IOException, JetStreamApiException {
    StreamInfo streamInfo = StreamManager.getStreamInfo(connection, streamConfiguration,
        pubOnlyConfiguration.isAutomaticallyCreateStream());
    if (streamInfo == null) {
      throw new StreamNotFoundException("Stream could not be found! Consider enabling "
          + "`automaticallyCreateStream` in `PubOnlyConfiguration`");
    }
  }

  private NatsMessage createNATSMessage(String subject, String message) {
    return NatsMessage.builder().subject(subject).data(message, StandardCharsets.UTF_8).build();
  }

  private void publishFailedMessages() {
    while (true) {
      synchronized (this) {
        try {
          // Retrieve and publish message at the head of the queue
          connectionPrimer.getActiveConnection().jetStream().publish(failedMessages.peek());
          // Remove message at the head of the queue
          failedMessages.poll();
        } catch (IOException | JetStreamApiException e) {
          e.printStackTrace();
        }

        // Break from loop if queue is empty or connection is null
        if (failedMessages.isEmpty() || connectionPrimer.getActiveConnection() == null) {
          break;
        }
      }
    }
  }

  // TODO remove this after testing, also remove dependency from pom.xml
  private void printObject(Object object) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      System.out.println(objectMapper.writeValueAsString(object));
    } catch (JsonProcessingException e) {
      // log an error
    }
  }

  @Override
  public void connectionUpdated(ConnectionPrimer connectionPrimer) {
    Connection connection = connectionPrimer.getActiveConnection();
    if (connection != null) {
      try {
        getJetStream(connection);
      } catch (IOException | JetStreamApiException | StreamNotFoundException e) {
        failedMessages.clear();
      }
      if (!failedMessages.isEmpty()) {
        publishFailedMessages();
      }
    }
  }
}
