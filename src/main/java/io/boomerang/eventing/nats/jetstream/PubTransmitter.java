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
import io.boomerang.eventing.nats.jetstream.exception.NoNatsConnectionException;
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

  private ConcurrentLinkedQueue<Message> failedMessages = new ConcurrentLinkedQueue<Message>(); 

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
  public void publish(String subject, String message)
      throws IOException, JetStreamApiException, StreamNotFoundException, SubjectMismatchException {

    // Check if the subject matches stream's wildcard subject
    Boolean subjectMatches = streamConfiguration.getSubjects().stream()
        .anyMatch(wildcard -> SubjectMatchChecker.doSubjectsMatch(subject, wildcard));

    if (!subjectMatches) {
      throw new SubjectMismatchException(
          "Subject \"" + subject + "\" does not match any subjects of the stream!");
    }

    // Get NATS connection
    Connection connection = connectionPrimer.getActiveConnection();
    if (connection == null) {
      storeMessage(subject, message);
      throw new NoNatsConnectionException("No connection to the NATS server!");
    }

    // Get Jetstream stream from the NATS server (this will also automatically create the stream if
    // not present on the server)
    try {
      getJetStream(connection);
    } catch (NullPointerException npe) {
      storeMessage(subject, message);
    }

    // Create the NATS message
    Message natsMessage = createNATSMessage(subject, message);

    // Publish the message
    PublishAck publishAck = connection.jetStream().publish(natsMessage);

    System.out.println("Message \"" + message + "\" with subject \"" + subject + "\" published to stream");
    printObject(publishAck);
    // logger.debug("Message published to the stream! " + publishAck);
  }

  private void getJetStream(Connection connection) throws IOException, JetStreamApiException {
    StreamInfo streamInfo = StreamManager.getStreamInfo(connection, streamConfiguration,
        pubOnlyConfiguration.isAutomaticallyCreateStream());

    if (streamInfo == null) {
      throw new StreamNotFoundException("Stream could not be found! Consider enabling "
          + "`automaticallyCreateStream` in `PubOnlyConfiguration`");
    }
  }

  // TODO replace this w one line where needed instead of a method call?
  private void storeMessage(String subject, String message) {
    System.out.println("Storing message to send later");
    failedMessages.add(createNATSMessage(subject, message));
    printObject(failedMessages);
  }

  // TODO replace this w one line where needed instead of a method call?
  private NatsMessage createNATSMessage(String subject, String message) {
    return NatsMessage.builder().subject(subject).data(message, StandardCharsets.UTF_8).build();
  }

  private void publishFailedMessages() {
    // removed unnecessary arguments from this method
    // convert failedMessages List<Message> -> ConcurrentLinkedQueue<Message>
    // remove one message at a time after successful publish
    // attempt actions inside synchronized while loop,
    // break if queue is empty or if connection fails again
    
    while (true) {
      synchronized (this) {
        try {
          System.out.println("Resending failed message");
          PublishAck publishAck =
              connectionPrimer.getActiveConnection().jetStream().publish(failedMessages.peek());
          System.out.println("PublishAck:");
          printObject(publishAck);
          if (publishAck != null) {
            System.out.println("-------------------------------------------------");
            System.out.println("Queue size before: " + failedMessages.size());
            printObject(failedMessages);
            
            System.out.println("Removing published message");
            failedMessages.poll();
            
            System.out.println("Queue size after: " + failedMessages.size());
            printObject(failedMessages);
            System.out.println("-------------------------------------------------");
          }
        } catch (IOException | JetStreamApiException e) {
          e.printStackTrace();
        }

        if (failedMessages == null || connectionPrimer.getActiveConnection() == null) {
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
    // TODO don't retry messages if failed stream
    Connection connection = connectionPrimer.getActiveConnection();
    if (connection != null) {
      System.out.println("Connection active");
      try {
        getJetStream(connection);
      } catch (Exception e) {
        e.printStackTrace();
      }
      publishFailedMessages();
    } else {
      System.out.println("Saving message(s) to send upon reconnection");
    }
  }
}
